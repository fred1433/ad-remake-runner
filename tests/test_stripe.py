"""Stripe events: replays and out-of-order delivery credit a payment once."""
import json
import threading

import pytest

from remake import credits, db
from tests.conftest import ACCOUNT

SECRET = "whsec_test"


def event(eid, etype, payment, amount=200):
    meta = {"account_id": ACCOUNT, "credits": str(amount)}
    obj = {"id": f"cs_{payment}", "payment_intent": payment, "payment_status": "paid", "metadata": meta} \
        if etype == "checkout.session.completed" else {"id": payment, "status": "succeeded", "metadata": meta}
    return {"id": eid, "type": etype, "data": {"object": obj}}


def purchases(conn, payment):
    return conn.execute("select * from ledger where idempotency_key=%s", (f"purchase:{payment}",)).fetchall()


def test_same_event_delivered_eight_times_at_once(dsn, conn):
    ev = event("evt_1", "checkout.session.completed", "pi_1")
    effects = []
    barrier = threading.Barrier(8)

    def deliver():
        with db.connect(dsn) as c:
            barrier.wait()
            effects.append(credits.handle_stripe_event(c, ev))

    ts = [threading.Thread(target=deliver) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sorted(effects) == ["credited 200"] + ["duplicate event: ignored"] * 7
    assert len(purchases(conn, "pi_1")) == 1
    assert credits.balance(conn, ACCOUNT)["balance"] == 700


def test_replayed_event_has_no_effect(dsn, conn):
    ev = event("evt_2", "checkout.session.completed", "pi_2")
    assert credits.handle_stripe_event(conn, ev) == "credited 200"
    assert credits.handle_stripe_event(conn, ev) == "duplicate event: ignored"
    assert len(purchases(conn, "pi_2")) == 1


@pytest.mark.parametrize("order", [("payment_intent.succeeded", "checkout.session.completed"),
                                   ("checkout.session.completed", "payment_intent.succeeded")],
                         ids=["payment intent first", "checkout first"])
def test_two_event_types_for_one_payment_in_either_order(dsn, conn, order):
    a = credits.handle_stripe_event(conn, event("evt_a", order[0], "pi_3"))
    b = credits.handle_stripe_event(conn, event("evt_b", order[1], "pi_3"))
    assert (a, b) == ("credited 200", "payment already credited")
    assert len(purchases(conn, "pi_3")) == 1


def test_signature_is_checked():
    payload = json.dumps(event("evt_s", "checkout.session.completed", "pi_s")).encode()
    header = credits.sign(payload, SECRET, ts=1_000_000)
    assert credits.verify(payload, header, SECRET, now=1_000_010)["id"] == "evt_s"
    with pytest.raises(ValueError, match="signature mismatch"):
        credits.verify(payload.replace(b"200", b"999"), header, SECRET, now=1_000_010)
    with pytest.raises(ValueError, match="outside tolerance"):
        credits.verify(payload, header, SECRET, now=1_000_000 + 301)


def test_signature_accepts_any_v1_during_a_secret_rotation():
    payload = json.dumps(event("evt_r", "checkout.session.completed", "pi_r")).encode()
    good = credits.sign(payload, SECRET, ts=1_000_000).split(",")[1]
    old = credits.sign(payload, "whsec_old", ts=1_000_000).split(",")[1]
    for header in (f"t=1000000,{good},{old}", f"t=1000000,{old},{good}", f"t=1000000, {old}, {good}"):
        assert credits.verify(payload, header, SECRET, now=1_000_010)["id"] == "evt_r"
    with pytest.raises(ValueError, match="signature mismatch"):
        credits.verify(payload, f"t=1000000,{old}", SECRET, now=1_000_010)
    with pytest.raises(ValueError, match="no timestamp or no v1"):
        credits.verify(payload, "v1=abc", SECRET, now=1_000_010)


def test_an_unpaid_checkout_credits_nothing_until_its_payment_succeeds(dsn, conn):
    ev = event("evt_u", "checkout.session.completed", "pi_u")
    ev["data"]["object"]["payment_status"] = "unpaid"  # a delayed payment method, not settled yet
    assert credits.handle_stripe_event(conn, ev) == "ignored: payment_status is 'unpaid', not 'paid'"
    assert purchases(conn, "pi_u") == []
    later = event("evt_u2", "checkout.session.completed", "pi_u")
    later["type"] = "checkout.session.async_payment_succeeded"
    assert credits.handle_stripe_event(conn, later) == "credited 200"
    assert len(purchases(conn, "pi_u")) == 1


def test_sessions_without_a_payment_reference_do_not_share_a_key(dsn, conn):
    for i in (1, 2):
        ev = event(f"evt_n{i}", "checkout.session.completed", None)
        ev["data"]["object"]["id"] = f"cs_free_{i}"
        assert credits.handle_stripe_event(conn, ev) == "ignored: no payment reference"
    assert purchases(conn, None) == []
    assert credits.balance(conn, ACCOUNT)["balance"] == 500


def test_an_event_without_credits_metadata_is_recorded_not_raised(dsn, conn):
    ev = {"id": "evt_other", "type": "payment_intent.succeeded",
          "data": {"object": {"id": "pi_other_flow", "status": "succeeded"}}}  # Stripe's default: no metadata
    for _ in range(2):  # Stripe would retry an error; here the second delivery is a plain duplicate
        effect = credits.handle_stripe_event(conn, ev)
    assert effect == "duplicate event: ignored"
    row = conn.execute("select effect from stripe_events where event_id='evt_other'").fetchone()
    assert row["effect"] == "ignored: no credits in metadata"


@pytest.mark.parametrize("amount", ["0", "-5", "12.5", "lots"])
def test_credits_must_be_a_positive_whole_number(dsn, conn, amount):
    ev = event("evt_bad_" + amount, "payment_intent.succeeded", "pi_bad")
    ev["data"]["object"]["metadata"]["credits"] = amount
    assert credits.handle_stripe_event(conn, ev).startswith("ignored: credits must be a positive whole number")
    assert credits.balance(conn, ACCOUNT)["balance"] == 500


def test_an_unknown_account_is_ignored(dsn, conn):
    ev = event("evt_acct", "payment_intent.succeeded", "pi_acct")
    ev["data"]["object"]["metadata"]["account_id"] = "acct_nobody"
    assert credits.handle_stripe_event(conn, ev) == "ignored: unknown account 'acct_nobody'"
