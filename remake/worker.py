"""Job runner: leases, fencing, retries with a ceiling, resumable steps.

A worker claims a job by taking a lease and bumping the job's fence number.
Every write it makes afterwards re-reads the job row under lock and checks that
the fence is still its own: a worker that stalled past its lease, while another
took the job over, can no longer change anything. If its own heartbeat cannot
renew the lease, the worker stops instead of working on without one.

Files follow the same rule: a step writes into a folder named after the job and
the worker's fence, and only the transaction that records the step as done
publishes them (by an atomic rename). A worker that lost the job can leave
files behind, never published ones.
"""
import json
import os
import shutil
import signal
import threading
import time
from contextlib import contextmanager

from . import credits, db
from .provider import ProviderRejected, ResponseLost


class Fenced(Exception):
    """Another worker holds this job now, or this worker lost its lease."""


class Transient(Exception):
    """Worth another attempt (provider down, provider failed the generation, a timeout)."""


class Exhausted(Transient):
    """Every attempt is spent: the step fails without sending anything more."""


class StopJob(Exception):
    """The job needs a person; the worker leaves it alone."""


def is_transient(err):
    return isinstance(err, (Transient, TimeoutError))


def log(conn, job_id, worker, kind, step=None, **detail):
    conn.execute("insert into events (job_id, worker, kind, step, detail) values (%s,%s,%s,%s,%s::jsonb)",
                 (job_id, worker, kind, step, json.dumps(detail)))


def create_job(conn, account_id, kind, params, step_names, cost):
    with conn.transaction():
        # Lock the account before the job row exists: the job's foreign key takes a
        # share lock on the account, and two creators upgrading it would deadlock.
        conn.execute("select id from accounts where id=%s for update", (account_id,))
        job_id = conn.execute("insert into jobs (account_id, kind, params) values (%s,%s,%s::jsonb) returning id",
                              (account_id, kind, json.dumps(params))).fetchone()["id"]
        credits.hold(conn, account_id, job_id, cost)
        for i, name in enumerate(step_names):
            conn.execute("insert into steps (job_id, name, seq) values (%s,%s,%s)", (job_id, name, i))
        log(conn, job_id, None, "job_created", credits_held=cost, steps=len(step_names))
    return job_id


def _settle_cancelled(conn, job_id, worker, why, previous):
    """Inside a transaction holding the job row: one outcome, the hold released."""
    released = credits.release(conn, job_id)
    conn.execute("update provider_calls set status='abandoned', updated_at=clock_timestamp()"
                 " where job_id=%s and status in ('intent','submitted','unknown')", (job_id,))
    conn.execute("update jobs set status='cancelled', cancel_requested=true, fence=fence+1, lease_owner=null,"
                 " lease_expires_at=null, updated_at=clock_timestamp() where id=%s", (job_id,))
    log(conn, job_id, worker, "job_cancelled", reservation_released=released, previous_status=previous, why=why)
    return "cancelled"


def cancel_job(conn, job_id):
    """Cancels a job. Returns its outcome: 'cancelled' when settled now (queued,
    waiting for review, or running with a lease that already ran out),
    'cancel_requested' when a live worker will stop at its next step, or the
    status of a job that had already finished."""
    with conn.transaction():
        j = conn.execute("select status, lease_expires_at < clock_timestamp() as expired from jobs"
                         " where id=%s for update", (job_id,)).fetchone()
        if j is None:
            raise KeyError(f"no job {job_id}")
        if j["status"] in ("queued", "needs_review"):
            return _settle_cancelled(conn, job_id, None, "cancelled by request", j["status"])
        if j["status"] == "running":
            if j["expired"]:
                return _settle_cancelled(conn, job_id, None, "cancelled by request; the worker's lease had run out",
                                         "running")
            conn.execute("update jobs set cancel_requested=true where id=%s", (job_id,))
            log(conn, job_id, None, "cancel_requested")
            return "cancel_requested"
        return j["status"]


def settle_abandoned_cancels(conn):
    """A cancel was requested, then the worker died: settled once its lease ran out."""
    done = []
    with conn.transaction():
        rows = conn.execute("select id from jobs where status='running' and cancel_requested"
                            " and lease_expires_at < clock_timestamp() for update skip locked").fetchall()
        for r in rows:
            done.append(_settle_cancelled(conn, r["id"], None, "cancel requested, then the worker's lease ran out",
                                          "running") and r["id"])
    return done


class Worker:
    def __init__(self, dsn, name, impl, provider=None, lease_s=8.0, backoff_base=1.0, backoff_cap=8.0,
                 max_attempts=4, poll_s=0.25, provider_wait_s=1.5, provider_deadline_s=600.0,
                 status_timeout_s=30.0, crash_at=None, stall_at=None, stall_s=0.0, on=None):
        self.dsn, self.name, self.impl, self.provider = dsn, name, impl, provider
        self.lease_s, self.backoff_base, self.backoff_cap = lease_s, backoff_base, backoff_cap
        self.max_attempts, self.poll_s, self.provider_wait_s = max_attempts, poll_s, provider_wait_s
        self.provider_deadline_s, self.status_timeout_s = provider_deadline_s, status_timeout_s
        self.crash_at, self.stall_at, self.stall_s = crash_at, stall_at, stall_s
        self.on = on or {}
        self.job = self.fence = None
        self._hb_stop = threading.Event()
        self._lost = threading.Event()
        self._lost_reason = None
        self._resumed = set()

    # ------------------------------------------------------------ claiming
    def claim(self, only_job=None):
        with db.connect(self.dsn) as c:
            settle_abandoned_cancels(c)
            row = c.execute(
                """with pick as (
                     select id, lease_owner as prev_owner, status as prev_status from jobs
                     where (status='queued'
                            or (status='running' and not cancel_requested and lease_expires_at < clock_timestamp()))
                       and (%s::bigint is null or id=%s)
                     order by id for update skip locked limit 1)
                   update jobs j set status='running', lease_owner=%s, fence=j.fence+1,
                     lease_expires_at=clock_timestamp() + make_interval(secs => %s), updated_at=clock_timestamp()
                   from pick where j.id=pick.id
                   returning j.id, j.fence, pick.prev_owner, pick.prev_status""",
                (only_job, only_job, self.name, self.lease_s)).fetchone()
            if row:
                kind = "lease_taken_over" if row["prev_status"] == "running" else "claimed"
                log(c, row["id"], self.name, kind, fence=row["fence"], previous_owner=row["prev_owner"],
                    lease_s=self.lease_s)
        return row

    def run_once(self, only_job=None):
        row = self.claim(only_job)
        if not row:
            return None
        return row["id"], self.process(row["id"], row["fence"])

    # ------------------------------------------------------------ lease
    def _heartbeat(self):
        try:
            with db.connect(self.dsn, app=f"heartbeat:{self.name}") as c:
                while not self._hb_stop.wait(self.lease_s / 3):
                    n = c.execute("update jobs set lease_expires_at=clock_timestamp() + make_interval(secs => %s)"
                                  " where id=%s and fence=%s and status='running'",
                                  (self.lease_s, self.job, self.fence)).rowcount
                    if n == 0:
                        self._lose("the job is no longer this worker's")
                        return
        except Exception as e:  # noqa: BLE001 - any failure means the lease is no longer renewed
            self._lose(f"heartbeat could not renew the lease ({type(e).__name__})")

    def _lose(self, why):
        if not self._hb_stop.is_set():
            self._lost_reason = why
            self._lost.set()

    @contextmanager
    def owned(self, c):
        if self._lost.is_set():
            raise Fenced(f"job {self.job}: lease lost, {self._lost_reason}; stopped before writing")
        with c.transaction():
            r = c.execute("select fence, status, cancel_requested from jobs where id=%s for update",
                          (self.job,)).fetchone()
            if r["fence"] != self.fence or r["status"] != "running":
                raise Fenced(f"job {self.job}: fence {self.fence} is stale (now {r['fence']}, {r['status']})")
            yield r

    def _checkpoint(self, point):
        if point in self.on:
            self.on[point](self)
        if self.crash_at == point:
            os.kill(os.getpid(), signal.SIGKILL)
        if self.stall_at == point:
            self._hb_stop.set()
            time.sleep(self.stall_s)

    # ------------------------------------------------------------ main loop
    def process(self, job_id, fence):
        self.job, self.fence = job_id, fence
        self._hb_stop.clear()
        self._lost.clear()
        self.impl.abort = self._lost  # long steps stop their subprocesses when the lease is lost
        hb = threading.Thread(target=self._heartbeat, daemon=True)
        hb.start()
        try:
            with db.connect(self.dsn) as c:
                return self._loop(c)
        except Fenced as e:
            with db.connect(self.dsn) as c:
                log(c, self.job, self.name, "fenced_out", reason=str(e))
            return "fenced"
        finally:
            self._hb_stop.set()

    def _loop(self, c):
        while True:
            with self.owned(c) as j:
                if j["cancel_requested"]:
                    return _settle_cancelled(c, self.job, self.name, "cancel requested", "running")
                step = c.execute("select *, greatest(0, extract(epoch from next_attempt_at - clock_timestamp()))"
                                 " as wait_s from steps where job_id=%s and status<>'done' order by seq limit 1",
                                 (self.job,)).fetchone()
            if step is None:
                self._checkpoint("before_succeed")
                return self._succeed(c)
            if step["wait_s"] and float(step["wait_s"]) > 0:
                self._sleep(float(step["wait_s"]))
                continue
            try:
                out = self._run_clip(c, step) if step["name"].startswith("clip:") else self._run_plain(c, step)
            except Fenced:
                raise
            except StopJob:
                return "needs_review"
            except Exception as e:  # noqa: BLE001 - every failure is classified below
                if self._fail_attempt(c, step, e):
                    return "failed"
                continue
            if out is None:
                continue
            self._checkpoint(f"{step['name']}:ran")
            self._done(c, step, out)
            self._checkpoint(step["name"])

    def _sleep(self, s):
        if self._lost.wait(s):
            raise Fenced(f"job {self.job}: lease lost, {self._lost_reason}")

    def _done(self, c, step, out):
        """Records the step and publishes its files in one transaction."""
        publish = out.pop("publish", None) or {}
        staged = []
        try:
            for dst, src in publish.items():
                tmp = f"{dst}.f{self.fence}.tmp"
                os.makedirs(os.path.dirname(tmp) or ".", exist_ok=True)
                shutil.copyfile(src, tmp)
                staged.append((tmp, dst))
            if publish:
                out["published"] = sorted(publish)
            with self.owned(c):
                c.execute("update steps set status='done', output=%s::jsonb, next_attempt_at=null, last_error=null"
                          " where job_id=%s and name=%s", (json.dumps(out), self.job, step["name"]))
                if out.get("request_id"):
                    c.execute("update provider_calls set status='done', updated_at=clock_timestamp()"
                              " where request_id=%s", (out["request_id"],))
                n = c.execute("select attempts from steps where job_id=%s and name=%s",
                              (self.job, step["name"])).fetchone()["attempts"]
                for tmp, dst in staged:
                    os.replace(tmp, dst)
                log(c, self.job, self.name, "step_done", step["name"], attempt=n,
                    **({"published": out["published"]} if publish else {}))
        finally:
            for tmp, _ in staged:
                if os.path.exists(tmp):
                    os.remove(tmp)

    def _outputs(self, c):
        rows = c.execute("select name, output from steps where job_id=%s and status='done'", (self.job,)).fetchall()
        return {r["name"]: r["output"] for r in rows}

    def _job(self, c):
        j = c.execute("select * from jobs where id=%s", (self.job,)).fetchone()
        return {**j, "fence": self.fence}  # this worker's fence, which names its files

    def _run_plain(self, c, step):
        with self.owned(c):
            c.execute("update steps set attempts=attempts+1 where job_id=%s and name=%s", (self.job, step["name"]))
            log(c, self.job, self.name, "step_started", step["name"], attempt=step["attempts"] + 1)
        return self.impl.run(self._job(c), step["name"], self._outputs(c))

    # ------------------------------------------------------------ clips
    def _needs_review(self, c, call, msg):
        with self.owned(c):
            c.execute("update provider_calls set status='unknown', updated_at=clock_timestamp() where client_key=%s",
                      (call["client_key"],))
            c.execute("update steps set status='outcome_unknown', last_error=%s where job_id=%s and name=%s",
                      (msg, self.job, call["step"]))
            c.execute("update jobs set status='needs_review', error=%s, lease_owner=null,"
                      " updated_at=clock_timestamp() where id=%s", (msg, self.job))
            log(c, self.job, self.name, "outcome_unknown", call["step"], client_key=call["client_key"])
        raise StopJob(msg)  # raised after the commit above

    def _lookup(self, c, call):
        """The provider's request for this client key, or None if it never received it."""
        try:
            return self.provider.lookup(call["client_key"])
        except NotImplementedError:
            self._needs_review(c, call, (
                f"{call['step']}: request {call['client_key']} was written down, but no answer came back and this"
                " provider cannot be asked by client key, so whether it accepted the request is unknown."
                " Not resubmitted, to avoid paying for it twice. Credits stay held until someone checks the"
                " provider dashboard; cancel_job then releases them."))

    def _record_submitted(self, c, call, rid, kind, **detail):
        with self.owned(c):
            c.execute("update provider_calls set request_id=%s, status='submitted',"
                      " submitted_at=coalesce(submitted_at, clock_timestamp()), updated_at=clock_timestamp()"
                      " where client_key=%s", (rid, call["client_key"]))
            log(c, self.job, self.name, kind, call["step"], request_id=rid, attempt=call["attempt"], **detail)

    def _send(self, c, call):
        name, key = call["step"], call["client_key"]
        spec = self.impl.clip_spec(self._job(c), name, self._outputs(c))
        with self.owned(c):
            pass  # the fence is checked again right before anything is sent
        self._checkpoint(f"{name}:send")
        try:
            rid = self.provider.submit(key, name, spec)
        except ProviderRejected as e:
            with self.owned(c):
                c.execute("update provider_calls set status='rejected', error=%s where client_key=%s", (str(e), key))
            raise Transient(f"provider refused the submission: {e}")
        except ResponseLost as e:
            with self.owned(c):
                log(c, self.job, self.name, "provider_response_lost", name, client_key=key, error=str(e))
            rid = self._lookup(c, call)
            if rid is None:
                msg = f"{name}: the connection dropped before the provider received the request"
                with self.owned(c):
                    c.execute("update provider_calls set status='lost', error=%s where client_key=%s", (msg, key))
                raise Transient(msg)
            self._record_submitted(c, call, rid, "provider_request_found", client_key=key)
            return rid
        self._record_submitted(c, call, rid, "provider_submitted")
        self._checkpoint(f"{name}:submitted")
        return rid

    def _run_clip(self, c, step):
        name = step["name"]
        call = c.execute("select * from provider_calls where job_id=%s and step=%s"
                         " and status in ('intent','submitted','done') order by attempt desc limit 1",
                         (self.job, name)).fetchone()
        if call is None:
            with self.owned(c):
                s = c.execute("select attempts, last_error from steps where job_id=%s and name=%s",
                              (self.job, name)).fetchone()
                if s["attempts"] >= self.max_attempts:  # the ceiling holds even after a crash
                    last = c.execute("select error from provider_calls where job_id=%s and step=%s and error is not null"
                                     " order by attempt desc limit 1", (self.job, name)).fetchone()
                    raise Exhausted((last and last["error"]) or s["last_error"] or "every attempt failed")
                attempt = c.execute("update steps set attempts=attempts+1, status='pending' where job_id=%s and name=%s"
                                    " returning attempts", (self.job, name)).fetchone()["attempts"]
                key = f"job{self.job}:{name}:a{attempt}"
                c.execute("insert into provider_calls (client_key, job_id, step, attempt, status)"
                          " values (%s,%s,%s,%s,'intent')", (key, self.job, name, attempt))
            self._checkpoint(f"{name}:intent")
            call = c.execute("select * from provider_calls where client_key=%s", (key,)).fetchone()
            rid = self._send(c, call)
        elif call["request_id"] is None:
            # written down by a worker that stopped before it learned the request id
            rid = self._lookup(c, call)
            if rid is not None:
                self._record_submitted(c, call, rid, "provider_request_found", client_key=call["client_key"])
            else:
                with self.owned(c):
                    log(c, self.job, self.name, "provider_request_absent", name, client_key=call["client_key"],
                        note="the provider never received it: sent now, with the same client key")
                rid = self._send(c, call)
        else:
            rid = call["request_id"]
            if call["status"] == "done":
                with self.owned(c):
                    log(c, self.job, self.name, "provider_result_recovered", name, request_id=rid,
                        note="the provider had answered; its result is fetched again, nothing is resubmitted")
            else:
                sub = c.execute("select worker from events where job_id=%s and kind in"
                                " ('provider_submitted','provider_request_found') and detail->>'request_id'=%s"
                                " order by id limit 1", (self.job, rid)).fetchone()
                if sub and sub["worker"] != self.name and rid not in self._resumed:
                    self._resumed.add(rid)
                    with self.owned(c):
                        log(c, self.job, self.name, "provider_polling_resumed", name, request_id=rid,
                            submitted_by=sub["worker"], note="request id found in the database: polled, not resubmitted")
        call = c.execute("select * from provider_calls where request_id=%s", (rid,)).fetchone()
        return self._poll(c, call)

    def _timed(self, fn, timeout):
        """fn() or None if it has not returned after `timeout` seconds (an HTTP call with a deadline)."""
        box = {}

        def go():
            try:
                box["v"] = fn()
            except Exception as e:  # noqa: BLE001
                box["e"] = e
        t = threading.Thread(target=go, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive():
            return None
        if "e" in box:
            raise box["e"]
        return box["v"]

    def _poll(self, c, call):
        name, rid = call["step"], call["request_id"]
        pass_end = time.monotonic() + self.provider_wait_s
        while True:
            # the deadline runs on the database clock from the submission, whatever the calls below do
            age = float(c.execute("select extract(epoch from clock_timestamp() - submitted_at) as s"
                                  " from provider_calls where request_id=%s", (rid,)).fetchone()["s"] or 0)
            if self.provider_deadline_s is not None and age > self.provider_deadline_s:
                self._timed(lambda: self.provider.cancel(rid), self.status_timeout_s)
                msg = (f"{name}: no answer from the provider {self.provider_deadline_s:g} s after submission;"
                       f" request {rid} cancelled")
                with self.owned(c):
                    c.execute("update provider_calls set status='expired', error=%s, updated_at=clock_timestamp()"
                              " where request_id=%s", (msg, rid))
                    log(c, self.job, self.name, "provider_deadline", name, request_id=rid,
                        deadline_s=self.provider_deadline_s)
                raise Transient(msg)
            budget = min(self.status_timeout_s, max(0.05, (self.provider_deadline_s or 1e9) - age))
            st = self._timed(lambda: self.provider.status(rid), budget)
            if st is None:
                with self.owned(c):
                    log(c, self.job, self.name, "provider_call_timeout", name, request_id=rid, timeout_s=round(budget, 2))
            elif st["state"] == "done":
                self._checkpoint(f"{name}:answered")
                return {"clip": st["result"], "request_id": rid}
            elif st["state"] in ("failed", "cancelled"):
                err = st.get("error") or st["state"]
                with self.owned(c):
                    c.execute("update provider_calls set status='failed', error=%s, updated_at=clock_timestamp()"
                              " where request_id=%s", (f"provider failed {name}: {err}", rid))
                    log(c, self.job, self.name, "provider_failed", name, request_id=rid, error=err)
                self._checkpoint(f"{name}:failed:a{call['attempt']}")
                raise Transient(f"provider failed {name}: {err}")
            if time.monotonic() > pass_end:
                with self.owned(c):
                    c.execute("update steps set status='waiting_provider',"
                              " next_attempt_at=clock_timestamp() + make_interval(secs => %s)"
                              " where job_id=%s and name=%s", (self.poll_s * 4, self.job, name))
                    log(c, self.job, self.name, "provider_slow", name, request_id=rid, waited_s=self.provider_wait_s,
                        note="not a failure: the same request is polled again, nothing is resubmitted")
                return None
            self._sleep(self.poll_s)

    def _fail_attempt(self, c, step, err):
        with self.owned(c):
            a = c.execute("select attempts from steps where job_id=%s and name=%s",
                          (self.job, step["name"])).fetchone()["attempts"]
            transient = is_transient(err)
            if not transient or a >= self.max_attempts:
                why = f"{type(err).__name__}: {err}" if not transient else str(err)
                msg = f"{step['name']} failed after {a} attempt{'s' if a != 1 else ''}: {why}"
                c.execute("update steps set status='failed', last_error=%s where job_id=%s and name=%s",
                          (msg, self.job, step["name"]))
                c.execute("update jobs set status='failed', error=%s, lease_owner=null, updated_at=clock_timestamp()"
                          " where id=%s", (msg, self.job))
                released = credits.release(c, self.job)
                log(c, self.job, self.name, "job_failed", step["name"], error=msg, reservation_released=released)
                return True
            delay = min(self.backoff_cap, self.backoff_base * 2 ** (a - 1))
            c.execute("update steps set next_attempt_at=clock_timestamp() + make_interval(secs => %s), last_error=%s"
                      " where job_id=%s and name=%s", (delay, str(err), self.job, step["name"]))
            log(c, self.job, self.name, "retry_scheduled", step["name"], attempt=a, next_in_s=delay,
                max_attempts=self.max_attempts, error=str(err))
            return False

    def _succeed(self, c):
        with self.owned(c) as r:
            if r["cancel_requested"]:  # one outcome: a cancel that arrived first wins, and nothing is charged
                return _settle_cancelled(c, self.job, self.name, "cancel arrived before the job was settled", "running")
            outputs = self._outputs(c)
            last = c.execute("select name from steps where job_id=%s order by seq desc limit 1", (self.job,)).fetchone()
            entry = credits.consume(c, self.job)
            c.execute("update jobs set status='succeeded', output=%s::jsonb, lease_owner=null, lease_expires_at=null,"
                      " updated_at=clock_timestamp() where id=%s", (json.dumps(outputs.get(last["name"])), self.job))
            log(c, self.job, self.name, "job_succeeded",
                charge=None if entry is None else {"id": entry["id"], "amount": entry["amount"],
                                                   "idempotency_key": entry["idempotency_key"]})
        return "succeeded"
