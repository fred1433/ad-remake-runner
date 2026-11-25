"""Credits: purchases from Stripe events, holds per job, one charge per job.

Unit: a fictional "credit". Balance = sum(ledger); available = balance - active holds.
"""
import hashlib
import hmac
import json
import re
import time

import psycopg


class NotEnoughCredits(Exception):
    pass


def balance(conn, account_id):
    row = conn.execute(
        "select coalesce((select sum(amount) from ledger where account_id=%s),0) as balance,"
        " coalesce((select sum(amount) from credit_holds where account_id=%s and status='held'),0) as held",
        (account_id, account_id)).fetchone()
    return {"balance": int(row["balance"]), "held": int(row["held"]),
            "available": int(row["balance"]) - int(row["held"])}


def hold(conn, account_id, job_id, amount):
    """Called inside the job-creation transaction. Locks the account row so two
    jobs created at once cannot both spend the same credits."""
    conn.execute("select id from accounts where id=%s for update", (account_id,))
    b = balance(conn, account_id)
    if b["available"] < amount:
        raise NotEnoughCredits(f"account {account_id} needs {amount} credits, has {b['available']} available")
    conn.execute("insert into credit_holds (job_id, account_id, amount, status) values (%s,%s,%s,'held')",
                 (job_id, account_id, amount))


def consume(conn, job_id):
    """Turn the hold into one charge. Safe to call twice: the second call finds
    no 'held' row, and the ledger key is unique anyway."""
    row = conn.execute(
        "update credit_holds set status='consumed', settled_at=clock_timestamp()"
        " where job_id=%s and status='held' returning account_id, amount", (job_id,)).fetchone()
    if not row:
        return None
    entry = conn.execute(
        "insert into ledger (account_id, job_id, kind, amount, idempotency_key)"
        " values (%s,%s,'charge',%s,%s) on conflict (idempotency_key) do nothing returning *",
        (row["account_id"], job_id, -row["amount"], f"charge:job:{job_id}")).fetchone()
    return entry


def release(conn, job_id):
    """Nothing was charged, so nothing is refunded: the hold is released."""
    row = conn.execute(
        "update credit_holds set status='released', settled_at=clock_timestamp()"
        " where job_id=%s and status='held' returning amount", (job_id,)).fetchone()
    return row["amount"] if row else 0


# ---------------------------------------------------------------- Stripe events

def sign(payload: bytes, secret: str, ts: int | None = None) -> str:
    """Builds a Stripe-Signature header (scheme v1: HMAC-SHA256 of "t.payload")."""
    ts = int(time.time()) if ts is None else ts
    mac = hmac.new(secret.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


def verify(payload: bytes, header: str, secret: str, tolerance: int = 300, now: int | None = None) -> dict:
    """Checks a Stripe-Signature header. During a secret rotation Stripe sends one
    v1 signature per active secret: the event is accepted if any of them matches."""
    ts, sigs = None, []
    for part in header.split(","):
        k, _, v = part.strip().partition("=")
        if k == "t":
            ts = int(v) if v.isdigit() else None
        elif k == "v1":
            sigs.append(v)
    if ts is None or not sigs:
        raise ValueError("signature header has no timestamp or no v1 signature")
    now = int(time.time()) if now is None else now
    if abs(now - ts) > tolerance:
        raise ValueError("signature timestamp outside tolerance")
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()
    if not any(hmac.compare_digest(expected, s) for s in sigs):
        raise ValueError("signature mismatch")
    return json.loads(payload)


CHECKOUT = {"checkout.session.completed", "checkout.session.async_payment_succeeded"}
CREDITING = CHECKOUT | {"payment_intent.succeeded"}


MAX_CREDITS = 1_000_000  # per purchase; also keeps the amount inside the ledger's integer column
CREDITS_TEXT = re.compile(r"[1-9][0-9]{0,6}")  # ASCII digits only, no sign, space or leading zero


class Malformed(Exception):
    pass


def _show(v):
    """A short, safe description of a value for the recorded reason."""
    if isinstance(v, str):
        r = repr(v)
        return r if len(r) <= 40 else r[:37] + "...'"
    return "None" if v is None else f"a {type(v).__name__}"


def envelope_problem(event):
    """Why this event cannot be stored or read, or None. Checked before the first
    INSERT: the payload is an object; id and type are non-empty text; data.object
    is an object; and every text in it, key or value, at any depth, has no NUL
    character and encodes as UTF-8 (no lone surrogate)."""
    if not isinstance(event, dict):
        return f"the payload is {_show(event)}, not an object"
    for k in ("id", "type"):
        if not isinstance(event.get(k), str) or not event[k]:
            return f"{k} must be non-empty text, got {_show(event.get(k))}"
    data = event.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("object"), dict):
        return "data.object must be an object"
    stack = [event]
    while stack:
        v = stack.pop()
        if isinstance(v, dict):
            stack.extend(v.keys())
            stack.extend(v.values())
        elif isinstance(v, list):
            stack.extend(v)
        elif isinstance(v, str):
            if "\x00" in v:
                return "a text contains a NUL character"
            try:
                v.encode("utf-8")
            except UnicodeEncodeError:
                return "a text is not valid Unicode (lone surrogate)"
    return None


def _text(v):
    return v if isinstance(v, str) and v else None


def _purchase(conn, event):
    """What this event would credit, or the reason it credits nothing."""
    obj = event["data"]["object"]
    if event["type"] in CHECKOUT:
        payment = _text(obj.get("payment_intent"))
        if obj.get("payment_status") != "paid":
            return payment, None, f"ignored: payment_status is {_show(obj.get('payment_status'))}, not 'paid'"
    else:
        payment = _text(obj.get("id"))
        if obj.get("status", "succeeded") != "succeeded":
            return payment, None, f"ignored: payment intent status is {_show(obj.get('status'))}"
    if not payment:
        return None, None, "ignored: no payment reference"
    meta = obj.get("metadata") or {}
    if not isinstance(meta, dict) or "credits" not in meta or "account_id" not in meta:
        return payment, None, "ignored: no credits in metadata"
    raw, account = meta["credits"], meta["account_id"]
    if not (isinstance(raw, str) and CREDITS_TEXT.fullmatch(raw) and int(raw) <= MAX_CREDITS):
        return payment, None, (f"ignored: credits must be a positive whole number up to {MAX_CREDITS}, written as"
                               f" text in ASCII digits with no sign, space or leading zero, got {_show(raw)}")
    if not _text(account):
        return payment, None, f"ignored: account_id must be non-empty text, got {_show(account)}"
    if not conn.execute("select 1 from accounts where id=%s", (account,)).fetchone():
        return payment, None, f"ignored: unknown account {_show(account)}"
    return payment, (account, int(raw)), None


def _apply(conn, event):
    with conn.transaction():
        seen = conn.execute(
            "insert into stripe_events (event_id, type, payment_ref, effect) values (%s,%s,null,'pending')"
            " on conflict (event_id) do nothing returning event_id",
            (event["id"], event["type"])).fetchone()
        if not seen:
            return "duplicate event: ignored"
        payment, effect = None, "ignored type"
        if event["type"] in CREDITING:
            payment, credit, why = _purchase(conn, event)
            if credit is None:
                effect = why
            else:
                account, amount = credit
                row = conn.execute(
                    "insert into ledger (account_id, kind, amount, idempotency_key) values (%s,'purchase',%s,%s)"
                    " on conflict (idempotency_key) do nothing returning id",
                    (account, amount, f"purchase:{payment}")).fetchone()
                effect = f"credited {amount}" if row else "payment already credited"
        conn.execute("update stripe_events set effect=%s, payment_ref=%s where event_id=%s",
                     (effect, payment, event["id"]))
        return effect


def _record_malformed(conn, event, why):
    """Records a malformed event under its id when that id can be stored; never raises,
    except on a database outage."""
    effect = f"ignored: malformed event ({why})"
    eid = event.get("id") if isinstance(event, dict) else None
    etype = event.get("type") if isinstance(event, dict) else None
    if envelope_problem({"id": eid, "type": etype if isinstance(etype, str) and etype else "?", "data": {"object": {}}}):
        return effect + "; not recorded: its id cannot be stored"
    try:
        with conn.transaction():
            row = conn.execute("insert into stripe_events (event_id, type, payment_ref, effect) values (%s,%s,null,%s)"
                               " on conflict (event_id) do nothing returning event_id",
                               (eid, etype if isinstance(etype, str) and etype else "?", effect)).fetchone()
        return effect if row else "duplicate event: ignored"
    except psycopg.OperationalError:
        raise
    except Exception as e:  # noqa: BLE001
        return effect + f"; not recorded ({type(e).__name__})"


def handle_stripe_event(conn, event) -> str:
    """Applies one verified event and returns what it did (I5).

    The whole envelope is checked before the first INSERT (envelope_problem). An
    event the runner does not use, or cannot read, is recorded with the reason and
    the webhook answers 200, so Stripe stops retrying it. Any exception other than
    a database connection or timeout error (psycopg.OperationalError) is caught
    and the event is classed "ignored: malformed event (unexpected <error>)". A
    database outage still raises, so the webhook answers an error and Stripe
    retries later instead of losing a paid purchase.

    Credits are keyed on the payment intent, not on the event: a replayed event,
    the same event delivered twice at once, or two event types for one payment
    arriving in either order all end in a single purchase row. A checkout
    session credits only once it is paid."""
    try:
        why = envelope_problem(event)
        if why:
            return _record_malformed(conn, event, why)
        return _apply(conn, event)
    except psycopg.OperationalError:
        raise
    except Exception as e:  # noqa: BLE001 - the safety net of I5
        return _record_malformed(conn, event, f"unexpected {type(e).__name__}")
