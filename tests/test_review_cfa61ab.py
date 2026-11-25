"""The scenarios of the second review of the invariants, run against commit cfa61ab.

Each test says whether that commit broke it ("broke cfa61ab") or kept it
("held at cfa61ab"). They are written so that they run on both versions: the
invariant is asserted first, with nothing but the job, the database and the
files on disk, then any check that needs the newer code (remake.export)."""
import errno
import json
import os
import shutil
import subprocess
import sys
import threading
import time

import psycopg
import pytest

from remake import credits, db
from remake import worker as W
from remake.provider import SimProvider
from remake.worker import cancel_job
from tests.conftest import ACCOUNT, charges, events, new_job, submissions
from tests.fakes import FakeFlow, make_worker
from tests.test_runner import ROOT, job


def hold(conn, job_id):
    return conn.execute("select status from credit_holds where job_id=%s", (job_id,)).fetchone()["status"]


def in_thread(fn):
    box = {}
    t = threading.Thread(target=lambda: box.update(r=fn()), daemon=True)
    t.start()
    return t, box


def out_files(root):
    d = root / "out"
    return sorted(p.name for p in d.iterdir()) if d.exists() else []


def run_module(*args):
    return subprocess.run([sys.executable, "-m", *map(str, args)], cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT)},
                          capture_output=True, text=True).returncode


# ------------------------------------------------------------------ I1

def test_a_long_step_past_the_deadline_is_failed_by_its_own_heartbeat(dsn, conn, tmp_path):
    """Broke cfa61ab: the heartbeat renewed the lease past the deadline; the job stayed
    'running' until the end of the step (review point 3)."""
    jid = new_job(conn)
    w = make_worker(dsn, "A", flow=FakeFlow(tmp_path, edit_s=6.0), job_deadline_s=1.0)  # lease 1 s, beat every 0.33 s
    t0 = time.monotonic()
    t, box = in_thread(w.run_once)
    try:
        seen = []
        while time.monotonic() - t0 < 2.0:
            seen.append(job(conn, jid)["status"])
            time.sleep(0.1)
        assert seen[-1] == "failed", f"job still {seen[-1]} 1 s past its deadline, in the middle of a 6 s step"
        t.join(1.0)
        assert not t.is_alive(), "the step was not stopped"
        assert box["r"][1] == "failed"
        j = job(conn, jid)
        assert "deadline" in j["error"] and "heartbeat" in j["error"]
        assert hold(conn, jid) == "released" and charges(conn, jid) == []
        assert out_files(tmp_path) == []
    finally:
        t.join(10)


def test_a_status_call_and_a_cancel_call_that_hang_are_bounded(dsn, conn, monkeypatch):
    """Held at cfa61ab."""
    monkeypatch.setattr(SimProvider, "cancel", lambda self, rid: time.sleep(20))
    jid = new_job(conn)
    w = make_worker(dsn, "A", faults={"clip:02": ["hang"]}, hang_s=20, status_timeout_s=0.3, provider_deadline_s=1.0,
                    max_attempts=2, job_deadline_s=3.0)
    t0 = time.monotonic()
    t, box = in_thread(w.run_once)
    t.join(8)
    assert not t.is_alive() and time.monotonic() - t0 < 8
    assert job(conn, jid)["status"] in ("failed", "succeeded")  # the retry may succeed; what matters is the bound


def test_a_write_blocked_on_the_job_row_lock_is_bounded(dsn, conn):
    """Held at cfa61ab."""
    jid = new_job(conn)
    with db.connect(dsn) as other:
        other.execute("begin")
        other.execute("select * from jobs where id=%s for update", (jid,))
        t0 = time.monotonic()
        with pytest.raises(psycopg.errors.QueryCanceled):
            with db.connect(dsn, statement_timeout_s=1.0) as c:
                c.execute("update jobs set error='x' where id=%s", (jid,))
        assert time.monotonic() - t0 < 2
        other.execute("rollback")


# ------------------------------------------------------------------ I2

@pytest.mark.parametrize("point", ["prep:ran", "clip:03:intent", "clip:03:send", "clip:03:submitted", "before_succeed"])
def test_a_worker_killed_at_the_same_point_every_time_does_not_keep_the_job_running(dsn, conn, point):
    """before_succeed broke cfa61ab: 7 workers killed, job 'running', credits held
    (review point 4a). The other points held."""
    jid = new_job(conn)
    kills = 0
    for _ in range(7):
        if run_module("tests.fakes", dsn, jid, point) != -9:
            break
        kills += 1
        time.sleep(1.1)  # the dead worker's 1 s lease runs out
    j = job(conn, jid)
    assert j["status"] in ("failed", "succeeded"), f"{kills} workers killed at {point}: job still {j['status']}"
    assert kills <= 4
    if j["status"] == "failed":
        assert hold(conn, jid) == "released" and charges(conn, jid) == []
        assert "picked up by 4 workers" in j["error"]
    else:
        assert hold(conn, jid) == "consumed" and len(charges(conn, jid)) == 1
        assert all(n == 1 for n in submissions(conn, jid).values())


def test_a_disk_error_at_publication_does_not_raise_or_keep_the_job_running(dsn, conn, tmp_path, monkeypatch):
    """Broke cfa61ab: the copy into out/ raised ENOSPC to the caller six times out of six,
    job 'running', credits held (review point 4b). Since then the worker copies nothing;
    a disk error belongs to the export, which raises to the command and leaves no file."""
    def disk_full(a, b, *args, **kw):
        raise OSError(errno.ENOSPC, "No space left on device", str(b))
    monkeypatch.setattr(shutil, "copyfile", disk_full)
    jid = new_job(conn)
    results = []
    for i in range(3):
        try:
            results.append(make_worker(dsn, f"w{i}", flow=FakeFlow(tmp_path)).run_once())
        except Exception as e:  # noqa: BLE001
            results.append(f"raised {type(e).__name__}")
        if job(conn, jid)["status"] != "running":
            break
        time.sleep(1.1)
    assert not any(isinstance(r, str) for r in results), results
    j = job(conn, jid)
    assert j["status"] == "succeeded" and hold(conn, jid) == "consumed" and len(charges(conn, jid)) == 1
    from remake.export import export_films
    with pytest.raises(OSError):
        export_films(conn, tmp_path / "out")
    assert out_files(tmp_path) == []  # no temporary file left
    monkeypatch.undo()
    assert export_films(conn, tmp_path / "out") == [(jid, f"film_{jid}.txt", "exported")]
    assert export_films(conn, tmp_path / "out") == [(jid, f"film_{jid}.txt", "unchanged")]
    assert job(conn, jid)["status"] == "succeeded"


# ------------------------------------------------------------------ I3

@pytest.mark.parametrize("mode", ["after_replace", "after_stage"])
def test_a_worker_killed_while_writing_into_out_then_cancelled(dsn, conn, tmp_path, mode):
    """Broke cfa61ab in both modes: killed after the rename (or the staging copy) into
    out/, before the commit, then cancelled: a film in out/ for a job 'cancelled' and
    not charged (review points 1 and 2)."""
    jid = new_job(conn)
    rc = run_module("tests.kill_in_publish", dsn, jid, tmp_path, mode)
    time.sleep(1.1)
    answer = cancel_job(conn, jid)
    j = job(conn, jid)
    assert out_files(tmp_path) == [], f"rc {rc}, job {j['status']}: the worker wrote {out_files(tmp_path)} into out/"
    from remake.export import export_films
    exported = export_films(conn, tmp_path / "out")
    if j["status"] == "succeeded":
        assert answer == "succeeded" and len(charges(conn, jid)) == 1
        assert exported == [(jid, f"film_{jid}.txt", "exported")]
    else:
        assert charges(conn, jid) == [] and exported == [] and out_files(tmp_path) == []


def test_a_cancel_in_the_middle_of_a_long_edit(dsn, conn, tmp_path):
    """Held at cfa61ab."""
    jid = new_job(conn)
    w = make_worker(dsn, "A", flow=FakeFlow(tmp_path, edit_s=2.0))
    t, box = in_thread(w.run_once)
    while conn.execute("select attempts from steps where job_id=%s and name='edit'", (jid,)).fetchone()["attempts"] == 0:
        time.sleep(0.05)
    time.sleep(0.5)
    assert cancel_job(conn, jid) == "cancel_requested"
    t.join(10)
    assert box["r"][1] == "cancelled" and job(conn, jid)["status"] == "cancelled"
    assert out_files(tmp_path) == [] and charges(conn, jid) == []


def test_a_delivered_file_that_cannot_be_read_fails_the_step_like_any_error(dsn, conn, tmp_path):
    """New with the export: the runner reads each delivered file back (SHA-256) inside
    the step, so an I/O error there is the step's failure, not an exception to the caller."""
    jid = new_job(conn)
    _, status = make_worker(dsn, "A", flow=FakeFlow(tmp_path, vanish=True)).run_once()
    j = job(conn, jid)
    assert status == "failed" and j["status"] == "failed"
    assert j["error"].startswith("edit failed after 1 attempt: FileNotFoundError")
    assert hold(conn, jid) == "released" and charges(conn, jid) == [] and j["published"] is None


def test_only_a_succeeded_job_can_carry_a_publication(dsn, conn):
    """New with the export: a check constraint, not only the code."""
    jid = new_job(conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute("""update jobs set published='[{"name": "x"}]' where id=%s""", (jid,))


# ------------------------------------------------------------------ I4

def test_an_evicted_worker_writes_nothing_after_the_takeover(dsn, conn, monkeypatch):
    """Broke cfa61ab: after the takeover by B, A still wrote a 'fenced_out' event (review point 7)."""
    monkeypatch.setattr(W.Worker, "_heartbeat", lambda self: time.sleep(30))
    jid = new_job(conn)
    a = make_worker(dsn, "A", faults={"clip:02": ["hang"]}, hang_s=4, status_timeout_s=4.0)
    t, box = in_thread(a.run_once)
    time.sleep(1.3)  # A is blocked in a status call; its frozen heartbeat let the lease run out
    monkeypatch.undo()
    b = make_worker(dsn, "B")
    tb, bb = in_thread(b.run_once)
    t.join(15)
    tb.join(30)
    assert box["r"][1] == "fenced" and bb["r"][1] == "succeeded"
    took = events(conn, jid, "lease_taken_over")[0]
    after = [(e["worker"], e["kind"]) for e in events(conn, jid) if e["id"] > took["id"] and e["worker"] == "A"]
    assert after == []


def test_a_frozen_heartbeat_during_the_last_step_publishes_nothing(dsn, conn, tmp_path, monkeypatch):
    """Held at cfa61ab (there, a heartbeat frozen during the copy into out/)."""
    real = shutil.copyfile

    def slow(a, b, *args, **kw):
        time.sleep(1.5)
        return real(a, b, *args, **kw)
    monkeypatch.setattr(W.Worker, "_heartbeat", lambda self: time.sleep(30))
    monkeypatch.setattr(shutil, "copyfile", slow)
    jid = new_job(conn)
    _, status = make_worker(dsn, "A", flow=FakeFlow(tmp_path, edit_s=1.0)).run_once()
    j = job(conn, jid)
    assert status == "fenced" and j["status"] == "running"
    assert out_files(tmp_path) == [] and charges(conn, jid) == [] and j.get("published") is None


# ------------------------------------------------------------------ I5

MALFORMED = {
    "NUL in the account": {"id": "e1", "type": "checkout.session.completed", "data": {"object": {
        "payment_intent": "pi1", "payment_status": "paid", "metadata": {"account_id": "acct\u0000", "credits": "5"}}}},
    "NUL in the payment reference": {"id": "e2", "type": "checkout.session.completed", "data": {"object": {
        "payment_intent": "pi\u0000", "payment_status": "unpaid"}}},
    "lone surrogate in the payment reference": {"id": "e3", "type": "payment_intent.succeeded", "data": {"object": {
        "id": "pi\ud800", "status": "requires_action"}}},
    "lone surrogate in the account": {"id": "e4", "type": "checkout.session.completed", "data": {"object": {
        "payment_intent": "pi4", "payment_status": "paid", "metadata": {"account_id": "a\ud800", "credits": "5"}}}},
    "NUL in the event id": {"id": "evt\u0000", "type": "customer.created", "data": {}},
    "type is a list": {"id": "e6", "type": ["checkout.session.completed"], "data": {}},
    "id is an object": {"id": {"x": 1}, "type": "customer.created"},
    "no id": {"type": "checkout.session.completed"},
    "payload is a list": [1, 2],
    "data.object is text": {"id": "e7", "type": "payment_intent.succeeded", "data": {"object": "pi_x"}},
}
REFUSED_CREDITS = {"huge integer": 10 ** 5000, "padded with spaces": " 7 ", "an integer": 7, "leading zero": "007",
                   "zero": "0", "a float": 1000.0, "plus sign": "+7", "trailing newline": "7\n"}


@pytest.mark.parametrize("case", list(MALFORMED))
def test_a_malformed_signed_event_is_classed_not_raised(dsn, conn, case):
    """NUL, lone surrogates and a bad envelope broke cfa61ab (DataError, UnicodeEncodeError,
    TypeError, ProgrammingError, KeyError; review point 5). Delivered through sign then
    verify, as the webhook would."""
    payload = json.dumps(MALFORMED[case]).encode()
    ev = credits.verify(payload, credits.sign(payload, "whsec_t"), "whsec_t")
    effect = credits.handle_stripe_event(conn, ev)
    assert effect.startswith("ignored"), effect
    assert credits.balance(conn, ACCOUNT)["balance"] == 500
    eid = ev.get("id") if isinstance(ev, dict) else None
    if isinstance(eid, str) and "\u0000" not in eid:
        assert conn.execute("select effect from stripe_events where event_id=%s", (eid,)).fetchone()["effect"] == effect


@pytest.mark.parametrize("case", list(REFUSED_CREDITS))
def test_credits_are_text_in_plain_ascii_digits_only(dsn, conn, case):
    """'padded with spaces', 'trailing newline' (both stripped) and 'an integer' broke
    cfa61ab's README, which said credits are strings of ASCII digits: each credited 7
    (review point 6). 'leading zero' credited 7 too; the old README allowed it, the
    rule is stricter since. The others held."""
    ev = {"id": "evt_c", "type": "checkout.session.completed", "data": {"object": {
        "payment_intent": "pi_c", "payment_status": "paid", "metadata": {"account_id": ACCOUNT, "credits": REFUSED_CREDITS[case]}}}}
    effect = credits.handle_stripe_event(conn, ev)
    assert effect.startswith("ignored"), effect
    assert credits.balance(conn, ACCOUNT)["balance"] == 500


def test_an_unexpected_error_is_classed_malformed_and_recorded(dsn, conn, monkeypatch):
    """The safety net: a case nobody foresaw."""
    def boom(conn, event):
        raise RuntimeError("unforeseen")
    monkeypatch.setattr(credits, "_purchase", boom)
    ev = {"id": "evt_net", "type": "payment_intent.succeeded", "data": {"object": {"id": "pi_net", "status": "succeeded"}}}
    effect = credits.handle_stripe_event(conn, ev)
    assert effect == "ignored: malformed event (unexpected RuntimeError)"
    assert conn.execute("select effect from stripe_events where event_id='evt_net'").fetchone()["effect"] == effect


def test_a_database_outage_still_raises_so_stripe_retries(dsn):
    c = db.connect(dsn)
    c.close()
    ev = {"id": "evt_down", "type": "payment_intent.succeeded", "data": {"object": {"id": "pi_down", "status": "succeeded"}}}
    with pytest.raises(psycopg.OperationalError):
        credits.handle_stripe_event(c, ev)


def test_the_export_refuses_a_build_copy_that_changed_after_success(dsn, conn, tmp_path):
    """New with the export: out/ only gets the bytes whose SHA-256 was recorded with 'succeeded'."""
    from remake.export import ExportError, export_films
    jid = new_job(conn)
    assert make_worker(dsn, "A", flow=FakeFlow(tmp_path)).run_once()[1] == "succeeded"
    (tmp_path / f"job{jid}" / "f1" / "film.txt").write_text("changed after the job succeeded")
    with pytest.raises(ExportError, match="no longer matches"):
        export_films(conn, tmp_path / "out")
    assert out_files(tmp_path) == []
