"""A fast stand-in for the film pipeline: same step names, no media work.

Its edit step writes a small file into the worker's own folder (job and fence)
and delivers it, like the real edit step does with the film: the runner
fingerprints it, records it as published when the job succeeds, and
remake.export copies it to <root>/out/ afterwards."""
import json
import sys
import time
from pathlib import Path

from remake import media
from remake.provider import SimProvider
from remake.worker import Worker

STEPS = ["prep"] + [f"clip:{i:02d}" for i in range(1, 7)] + ["edit"]
COST = 60


class FakeFlow:
    abort = None

    def __init__(self, root=None, edit_s=0.0, stuck=None, vanish=False):
        self.root, self.edit_s, self.stuck, self.vanish = (Path(root) if root else None), edit_s, stuck, vanish

    def run(self, job, step, outputs):
        if step == self.stuck:  # a subprocess that never finishes on its own
            media.run(["sleep", "31.4159"], timeout=0.5, abort=self.abort)
        if step == "edit" and self.root:
            end = time.monotonic() + self.edit_s
            while time.monotonic() < end:  # a long encode, stopped like the real one when the lease is lost
                if self.abort is not None and self.abort.is_set():
                    raise media.Aborted("edit stopped: the worker lost its lease")
                time.sleep(0.02)
            d = self.root / f"job{job['id']}" / f"f{job['fence']}"
            d.mkdir(parents=True, exist_ok=True)
            (d / "film.txt").write_text(f"rendered under fence {job['fence']}")
            if self.vanish:  # the file is gone before the runner reads it back
                (d / "film.txt").unlink()
            return {"step": step, "film": str(d / "film.txt"), "deliver": {f"film_{job['id']}.txt": str(d / "film.txt")}}
        return {"step": step, "seen": sorted(outputs)}

    def clip_spec(self, job, step, outputs):
        return {"job": job["id"], "beat": int(step.split(":")[1])}

    def render(self, spec, request_id):
        return f"clip_{spec['beat']:02d}_{request_id}.mp4"


def make_worker(dsn, name, faults=None, lookup=True, flow=None, slow_s=0.3, hang_s=5.0, **kw):
    flow = flow or FakeFlow()
    kw.setdefault("lease_s", 1.0)
    kw.setdefault("backoff_base", 0.05)
    kw.setdefault("backoff_cap", 0.4)
    kw.setdefault("poll_s", 0.02)
    kw.setdefault("provider_wait_s", 0.05)
    kw.setdefault("provider_deadline_s", 30.0)
    kw.setdefault("status_timeout_s", 5.0)
    kw.setdefault("job_deadline_s", 60.0)
    return Worker(dsn, name, flow, SimProvider(dsn, flow.render, faults or {}, supports_lookup=lookup,
                                               slow_s=slow_s, hang_s=hang_s), **kw)


if __name__ == "__main__":  # python -m tests.fakes DSN JOB CRASH_AT [FAULTS_JSON]
    dsn, job, crash_at = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    faults = json.loads(sys.argv[4]) if len(sys.argv) > 4 else None
    make_worker(dsn, "worker-killed", faults=faults, crash_at=crash_at).run_once(only_job=job)
