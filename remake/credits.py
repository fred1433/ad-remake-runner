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
    parts = dict(p.split("=", 1) for p in header.split(",") if "=" in p)
    ts = int(parts.get("t", "0"))
    now = int(time.time()) if now is None else now
    if abs(now - ts) > tolerance:
        raise ValueError("signature timestamp outside tolerance")
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, parts.get("v1", "")):
        raise ValueError("signature mismatch")
    return json.loads(payload)


CREDITING = {"checkout.session.completed", "payment_intent.succeeded"}


def handle_stripe_event(conn, event: dict) -> str:
    """Applies one verified event. Returns what it did.

    Credits are keyed on the payment intent, not on the event: a replayed event,
    the same event delivered twice at once, or the two event types for one
    payment arriving in either order all end in a single purchase row."""
    obj = event["data"]["object"]
    payment = obj.get("payment_intent") if event["type"] == "checkout.session.completed" else obj.get("id")
    with conn.transaction():
        seen = conn.execute(
            "insert into stripe_events (event_id, type, payment_ref, effect) values (%s,%s,%s,'pending')"
            " on conflict (event_id) do nothing returning event_id",
            (event["id"], event["type"], payment)).fetchone()
        if not seen:
            return "duplicate event: ignored"
        effect = "ignored type"
        if event["type"] in CREDITING:
            meta = obj.get("metadata") or {}
            row = conn.execute(
                "insert into ledger (account_id, kind, amount, idempotency_key) values (%s,'purchase',%s,%s)"
                " on conflict (idempotency_key) do nothing returning id",
                (meta["account_id"], int(meta["credits"]), f"purchase:{payment}")).fetchone()
            effect = f"credited {meta['credits']}" if row else "payment already credited"
        conn.execute("update stripe_events set effect=%s where event_id=%s", (effect, event["id"]))
        return effect
