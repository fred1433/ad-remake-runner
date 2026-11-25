"""Credits: purchases from Stripe events, holds per job, one charge per job.

Unit: a fictional "credit". Balance = sum(ledger); available = balance - active holds.
"""
import hashlib
import hmac
import json
import time


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


def _purchase(conn, event):
    """What this event would credit, or the reason it credits nothing."""
    obj = event["data"]["object"]
    if event["type"] in CHECKOUT:
        payment = obj.get("payment_intent")
        if not isinstance(payment, str):
            payment = None
        if obj.get("payment_status") != "paid":
            return payment, None, f"ignored: payment_status is {obj.get('payment_status')!r}, not 'paid'"
    else:
        payment = obj.get("id")
        if not isinstance(payment, str):
            payment = None
        if obj.get("status", "succeeded") != "succeeded":
            return payment, None, f"ignored: payment intent status is {obj.get('status')!r}"
    if not payment:
        return None, None, "ignored: no payment reference"
    meta = obj.get("metadata") or {}
    if not isinstance(meta, dict) or "credits" not in meta or "account_id" not in meta:
        return payment, None, "ignored: no credits in metadata"
    raw, account = meta["credits"], meta["account_id"]
    text = str(raw).strip() if isinstance(raw, (str, int)) and not isinstance(raw, bool) else ""
    if not (text.isascii() and text.isdigit()) or not 1 <= int(text) <= MAX_CREDITS:
        return payment, None, f"ignored: credits must be a positive whole number up to {MAX_CREDITS}, got {raw!r}"
    if not isinstance(account, str):
        return payment, None, f"ignored: account_id must be text, got {type(account).__name__}"
    if not conn.execute("select 1 from accounts where id=%s", (account,)).fetchone():
        return payment, None, f"ignored: unknown account {account!r}"
    return payment, (account, int(text)), None


def handle_stripe_event(conn, event: dict) -> str:
    """Applies one verified event and returns what it did. Never raises on an
    event it does not use (I5): that event is recorded with the reason, and the
    webhook answers 200 so Stripe stops retrying it.

    Credits are keyed on the payment intent, not on the event: a replayed event,
    the same event delivered twice at once, or two event types for one payment
    arriving in either order all end in a single purchase row. A checkout
    session credits only once it is paid."""
    with conn.transaction():
        seen = conn.execute(
            "insert into stripe_events (event_id, type, payment_ref, effect) values (%s,%s,null,'pending')"
            " on conflict (event_id) do nothing returning event_id",
            (event["id"], event["type"])).fetchone()
        if not seen:
            return "duplicate event: ignored"
        payment, effect = None, "ignored type"
        if event["type"] in CREDITING:
            try:
                payment, credit, why = _purchase(conn, event)
            except (KeyError, TypeError, AttributeError, ValueError) as e:
                payment, credit, why = None, None, f"ignored: malformed event ({type(e).__name__})"
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
