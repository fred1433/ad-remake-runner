"""Properties of the job runner, checked against a real Postgres."""
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from remake import credits, db
from remake.worker import Fenced, Worker, cancel_job
from tests.conftest import ACCOUNT, charges, events, new_job, submissions
from tests.fakes import COST, make_worker

ROOT = Path(__file__).resolve().parent.parent
CLIPS = [f"clip:{i:02d}" for i in range(1, 7)]


def steps(conn, job_id):
    return {r["name"]: r for r in conn.execute("select * from steps where job_id=%s", (job_id,)).fetchall()}


def job(conn, job_id):
    return conn.execute("select * from jobs where id=%s", (job_id,)).fetchone()


def kill_worker_at(dsn, job_id, point, faults=None):
    r = subprocess.run([sys.executable, "-m", "tests.fakes", dsn, str(job_id), point] + ([json.dumps(faults)] if faults else []), cwd=ROOT,
                       env={**os.environ, "PYTHONPATH": str(ROOT)}, capture_output=True, text=True)
    assert r.returncode == -9, r.stderr  # really killed by SIGKILL, no cleanup ran


# ------------------------------------------------------------------ one failed clip

def test_only_the_failed_clip_is_redone(dsn, conn):
    jid = new_job(conn)
    _, status = make_worker(dsn, "w1", faults={"clip:04": ["fail"]}).run_once()
    assert status == "succeeded"
    assert submissions(conn, jid) == {c: (2 if c == "clip:04" else 1) for c in CLIPS}
    st = steps(conn, jid)
    assert st["clip:04"]["attempts"] == 2
    assert all(st[c]["attempts"] == 1 for c in CLIPS if c != "clip:04")
    retry = events(conn, jid, "retry_scheduled")
    assert [(e["step"], e["detail"]["attempt"]) for e in retry] == [("clip:04", 1)]
    # the edit saw every clip, each from the provider's final answer
    assert st["edit"]["status"] == "done"


def test_one_charge_with_its_ledger_line(dsn, conn):
    jid = new_job(conn)
    make_worker(dsn, "w1", faults={"clip:04": ["fail"]}).run_once()
    ch = charges(conn, jid)
    assert len(ch) == 1 and ch[0]["amount"] == -COST and ch[0]["idempotency_key"] == f"charge:job:{jid}"
    assert credits.balance(conn, ACCOUNT) == {"balance": 500 - COST, "held": 0, "available": 500 - COST}
    # settling again is a no-op
    with conn.transaction():
        assert credits.consume(conn, jid) is None
    assert len(charges(conn, jid)) == 1


def test_retry_ceiling_gives_a_clear_error_and_releases_the_reservation(dsn, conn):
    jid = new_job(conn)
    assert credits.balance(conn, ACCOUNT)["available"] == 500 - COST
    _, status = make_worker(dsn, "w1", faults={"clip:03": ["fail"] * 4}, max_attempts=4).run_once()
    assert status == "failed"
    j = job(conn, jid)
    assert j["error"].startswith("clip:03 failed after 4 attempts: provider failed clip:03: 503")
    assert submissions(conn, jid) == {"clip:01": 1, "clip:02": 1, "clip:03": 4}
    assert charges(conn, jid) == []
    hold = conn.execute("select status from credit_holds where job_id=%s", (jid,)).fetchone()
    assert hold["status"] == "released"
    assert credits.balance(conn, ACCOUNT)["available"] == 500


def test_backoff_doubles_up_to_its_cap(dsn, conn):
    jid = new_job(conn)
    make_worker(dsn, "w1", faults={"clip:01": ["fail"] * 5}, max_attempts=6,
                backoff_base=0.05, backoff_cap=0.2).run_once()
    delays = [e["detail"]["next_in_s"] for e in events(conn, jid, "retry_scheduled")]
    assert delays == [0.05, 0.1, 0.2, 0.2, 0.2]
    assert job(conn, jid)["status"] == "succeeded"


def test_a_bug_is_not_retried(dsn, conn):
    jid = new_job(conn)
    w = make_worker(dsn, "w1")
    w.impl.run = lambda job, step, outputs: (_ for _ in ()).throw(KeyError("voice"))
    _, status = w.run_once()
    assert status == "failed"
    assert job(conn, jid)["error"] == "prep failed after 1 attempt: KeyError: 'voice'"
    assert credits.balance(conn, ACCOUNT)["available"] == 500


# ------------------------------------------------------------------ worker killed, lease, fencing

def test_killed_worker_job_is_taken_over_after_the_lease(dsn, conn):
    jid = new_job(conn)
    kill_worker_at(dsn, jid, "clip:02")
    j = job(conn, jid)
    assert j["status"] == "running" and j["lease_owner"] == "worker-killed"
    w = make_worker(dsn, "w2")
    assert w.claim() is None  # lease still valid: nobody may take it yet
    time.sleep(1.1)
    _, status = w.run_once()
    assert status == "succeeded"
    assert job(conn, jid)["fence"] == 2
    took = events(conn, jid, "lease_taken_over")
    assert took[0]["detail"]["previous_owner"] == "worker-killed"
    assert submissions(conn, jid) == {c: 1 for c in CLIPS}  # clips 1 and 2 were kept, not redone
    assert len(charges(conn, jid)) == 1


def test_killed_after_submit_polls_the_same_request(dsn, conn):
    jid = new_job(conn)
    kill_worker_at(dsn, jid, "clip:03:submitted")
    time.sleep(1.1)
    _, status = make_worker(dsn, "w2").run_once()
    assert status == "succeeded"
    assert submissions(conn, jid)["clip:03"] == 1
    assert [e["step"] for e in events(conn, jid, "provider_polling_resumed")] == ["clip:03"]


def test_a_stalled_worker_cannot_write_after_takeover(dsn, conn):
    jid = new_job(conn)
    slow = make_worker(dsn, "stalled", stall_at="clip:02", stall_s=2.5)
    result = {}
    t = threading.Thread(target=lambda: result.update(r=slow.run_once()))
    t.start()
    time.sleep(1.6)  # its heartbeat stopped at clip:02; the 1 s lease has run out
    _, status = make_worker(dsn, "rescuer").run_once()
    assert status == "succeeded"
    t.join()
    assert result["r"][1] == "fenced"
    assert events(conn, jid, "fenced_out")[0]["worker"] == "stalled"
    assert len(charges(conn, jid)) == 1
    assert submissions(conn, jid) == {c: 1 for c in CLIPS}


def test_stale_fence_is_refused_at_the_database(dsn, conn):
    jid = new_job(conn)
    old = make_worker(dsn, "old")
    old.claim()
    conn.execute("update jobs set lease_expires_at=clock_timestamp() - interval '1 second' where id=%s", (jid,))
    new = make_worker(dsn, "new")
    assert new.claim()["fence"] == 2
    old.job, old.fence = jid, 1
    with pytest.raises(Fenced):
        with old.owned(conn):
            pass


def test_two_workers_never_claim_the_same_job(dsn, conn):
    jid = new_job(conn)
    got = []
    barrier = threading.Barrier(6)

    def go(i):
        w = make_worker(dsn, f"w{i}")
        barrier.wait()
        got.append(w.claim())

    ts = [threading.Thread(target=go, args=(i,)) for i in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sum(1 for g in got if g) == 1


# ------------------------------------------------------------------ provider timing

def test_slow_provider_is_not_a_failure(dsn, conn):
    jid = new_job(conn)
    _, status = make_worker(dsn, "w1", faults={"clip:02": ["slow"]}).run_once()
    assert status == "succeeded"
    assert events(conn, jid, "provider_slow")
    assert events(conn, jid, "retry_scheduled") == []
    assert submissions(conn, jid)["clip:02"] == 1


def test_lost_answer_is_found_by_client_key(dsn, conn):
    jid = new_job(conn)
    _, status = make_worker(dsn, "w1", faults={"clip:03": ["lost_response"]}).run_once()
    assert status == "succeeded"
    assert events(conn, jid, "provider_request_found")
    assert submissions(conn, jid)["clip:03"] == 1


def test_lost_answer_without_lookup_waits_for_a_person(dsn, conn):
    jid = new_job(conn)
    _, status = make_worker(dsn, "w1", faults={"clip:03": ["lost_response"]}, lookup=False).run_once()
    assert status == "needs_review"
    j = job(conn, jid)
    assert "Not resubmitted, to avoid paying for it twice" in j["error"]
    assert steps(conn, jid)["clip:03"]["status"] == "outcome_unknown"
    assert submissions(conn, jid)["clip:03"] == 1
    hold = conn.execute("select status from credit_holds where job_id=%s", (jid,)).fetchone()
    assert hold["status"] == "held" and charges(conn, jid) == []
    assert make_worker(dsn, "w2").claim() is None  # no worker picks it up again on its own


# ------------------------------------------------------------------ credits and cancel

def test_cancel_queued_job_releases_its_reservation(dsn, conn):
    jid = new_job(conn)
    assert cancel_job(conn, jid) == "cancelled"
    assert credits.balance(conn, ACCOUNT)["available"] == 500
    assert make_worker(dsn, "w1").claim() is None


def test_cancel_running_job_stops_at_the_next_step(dsn, conn):
    jid = new_job(conn)
    w = make_worker(dsn, "w1")
    row = w.claim()
    assert cancel_job(conn, jid) == "cancel_requested"
    assert w.process(row["id"], row["fence"]) == "cancelled"
    assert charges(conn, jid) == []
    assert credits.balance(conn, ACCOUNT) == {"balance": 500, "held": 0, "available": 500}


def test_not_enough_credits_creates_nothing(dsn, conn):
    with pytest.raises(credits.NotEnoughCredits, match="needs 600 credits, has 500 available"):
        new_job(conn, cost=600)
    assert conn.execute("select count(*) as n from jobs").fetchone()["n"] == 0


def test_two_jobs_cannot_spend_the_same_credits(dsn, conn):
    errors = []

    def go():
        with db.connect(dsn) as c:
            try:
                new_job(c, cost=300)
            except Exception as e:  # noqa: BLE001 - a deadlock must fail the test too
                errors.append(e)

    ts = [threading.Thread(target=go) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(errors) == 1 and isinstance(errors[0], credits.NotEnoughCredits), errors
    assert credits.balance(conn, ACCOUNT)["held"] == 300
