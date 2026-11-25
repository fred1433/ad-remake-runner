"""Stripe events: replays and out-of-order delivery credit a payment once."""
import json
import threading

import pytest

from remake import credits, db
from tests.conftest import ACCOUNT

SECRET = "whsec_test"


def event(eid, etype, payment, amount=200):
    meta = {"account_id": ACCOUNT, "credits": str(amount)}
    obj = {"id": "cs_" + payment, "payment_intent": payment, "metadata": meta} if etype == "checkout.session.completed" \
        else {"id": payment, "metadata": meta}
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
