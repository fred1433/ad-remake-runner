"""The clip provider, simulated.

In production this is Seedance, Kling or Veo behind an HTTP API: submit a job,
get a request id, poll its status. None of them is wired in this repository.
Here the "remote" work is our local motion renderer (media.render_clip), and the
provider's own bookkeeping lives in the sim_provider schema so it survives a
worker being killed, as a real remote service would.

Faults are injected per step and per submission ordinal, e.g.
{"clip:04": ["fail"]}                first submission fails, second is clean
{"clip:05": ["slow"]}                accepted, answers only slow_s seconds later
{"clip:03": ["lost_response"]}       accepted, but the HTTP answer never arrives
{"clip:03": ["lost_before_accept"]}  the connection drops before the provider gets it
{"clip:02": ["pending_forever"]}     accepted, then "running" for ever
{"clip:02": ["hang"]}                accepted, then every status call hangs for hang_s
"""
import json
import time
import uuid

from . import db


class ProviderRejected(Exception):
    """The provider refused the submission: nothing was accepted, safe to retry."""


class ResponseLost(Exception):
    """The connection dropped after sending: the provider may have accepted it."""


class SimProvider:
    def __init__(self, dsn, render, faults=None, supports_lookup=True, slow_s=4.0, hang_s=3600.0):
        self.dsn = dsn
        self.slow_s = slow_s
        self.hang_s = hang_s
        self.render = render
        self.faults = faults or {}
        self.supports_lookup = supports_lookup

    def _fault_for(self, c, step, client_key):
        job = client_key.split(":")[0]
        n = c.execute("select count(*) as n from sim_provider.requests where step=%s and client_key like %s",
                      (step, job + ":%")).fetchone()["n"]
        lost = c.execute("select count(*) as n from sim_provider.dropped where step=%s and client_key like %s",
                         (step, job + ":%")).fetchone()["n"]
        plan = self.faults.get(step, [])
        k = n + lost
        return plan[k] if k < len(plan) else None

    def submit(self, client_key, step, spec):
        with db.connect(self.dsn) as c:
            existing = c.execute("select request_id from sim_provider.requests where client_key=%s",
                                 (client_key,)).fetchone()
            if existing:  # same client key: the provider answers with the same request
                return existing["request_id"]
            fault = self._fault_for(c, step, client_key)
            if fault == "lost_before_accept":
                c.execute("insert into sim_provider.dropped (client_key, step) values (%s,%s)", (client_key, step))
                raise ResponseLost(f"connection reset while sending {step}")
            rid = "req_" + uuid.uuid4().hex[:12]
            row = c.execute("insert into sim_provider.requests (request_id, client_key, step, spec, fault)"
                            " values (%s,%s,%s,%s::jsonb,%s) on conflict (client_key) do nothing returning request_id",
                            (rid, client_key, step, json.dumps(spec), fault)).fetchone()
            if row is None:  # a concurrent submission with the same key won
                return c.execute("select request_id from sim_provider.requests where client_key=%s",
                                 (client_key,)).fetchone()["request_id"]
        if fault == "lost_response":
            raise ResponseLost(f"connection reset after submitting {step}")
        return rid

    def lookup(self, client_key):
        """The request this client key created, or None if the provider never received it."""
        if not self.supports_lookup:
            raise NotImplementedError("this provider cannot be asked by client key")
        with db.connect(self.dsn) as c:
            row = c.execute("select request_id from sim_provider.requests where client_key=%s",
                            (client_key,)).fetchone()
        return row["request_id"] if row else None

    def cancel(self, request_id):
        with db.connect(self.dsn) as c:
            c.execute("update sim_provider.requests set state='cancelled' where request_id=%s and state not in ('done','failed')",
                      (request_id,))

    def status(self, request_id):
        with db.connect(self.dsn) as c:
            f = c.execute("select fault, state from sim_provider.requests where request_id=%s", (request_id,)).fetchone()
            if f and f["fault"] == "hang" and f["state"] not in ("done", "failed", "cancelled"):
                time.sleep(self.hang_s)  # a call that does not come back; no lock held meanwhile
                return {"state": "running"}
            # one poller at a time per request, as the provider would serialise it
            with c.transaction():
                r = c.execute("select * from sim_provider.requests where request_id=%s for update",
                              (request_id,)).fetchone()
                if r is None:
                    return {"state": "failed", "error": "unknown request id"}
                c.execute("update sim_provider.requests set polls=polls+1 where request_id=%s", (request_id,))
                if r["state"] in ("done", "failed", "cancelled"):
                    return {"state": r["state"], "result": r["result"], "error": r["error"]}
                if r["fault"] == "pending_forever":
                    return {"state": "running"}
                if r["fault"] == "fail":
                    c.execute("update sim_provider.requests set state='failed', error=%s where request_id=%s",
                              ("503 no capacity, generation not started", request_id))
                    return {"state": "failed", "error": "503 no capacity, generation not started"}
                if r["fault"] == "slow":
                    age = c.execute("select extract(epoch from clock_timestamp() - created_at) as s"
                                    " from sim_provider.requests where request_id=%s", (request_id,)).fetchone()["s"]
                    if float(age) < self.slow_s:
                        return {"state": "running"}
                out = self.render(r["spec"], request_id)
                c.execute("update sim_provider.requests set state='done', result=%s where request_id=%s",
                          (out, request_id))
                return {"state": "done", "result": out}

    def submissions(self, job_id):
        with db.connect(self.dsn) as c:
            rows = c.execute("select step, count(*) as n from sim_provider.requests where client_key like %s"
                             " group by step", (f"job{job_id}:%",)).fetchall()
        return {r["step"]: r["n"] for r in rows}
