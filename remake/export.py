"""out/ is an export of the database, made after the jobs ran (I3).

For every job in status 'succeeded', each file of its 'published' column is
copied from its immutable build folder (build/job<id>/f<fence>/) to out/<name>,
checked against its recorded SHA-256 before and after the copy. Only succeeded
jobs have a 'published' column (a check constraint in the schema), so nothing
from a failed, cancelled or running job can reach out/.

Idempotent: a file already in out/ with the right SHA-256 is left alone, so a
second export changes nothing. Two jobs that deliver the same name: the one with
the higher id is copied last and stays. The copy goes to a hidden temporary
file in out/, then an atomic rename; on any error the temporary file is removed
and the error is raised to the caller (the job's status is not touched).
"""
import hashlib
import os
import shutil
from pathlib import Path


class ExportError(Exception):
    pass


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def export_films(conn, out_dir, job_ids=None):
    """Returns [(job_id, name, 'exported' | 'unchanged')]."""
    out_dir = Path(out_dir)
    rows = conn.execute("select id, published from jobs where status='succeeded' and published is not null"
                        " and (%s::bigint[] is null or id = any(%s::bigint[])) order by id",
                        (job_ids, job_ids)).fetchall()
    done = []
    for r in rows:
        for f in r["published"]:
            name = f["name"]
            if not name or name != os.path.basename(name) or name in (".", "..") or name.startswith("."):
                raise ExportError(f"job {r['id']}: {name!r} is not a plain file name")
            dst = out_dir / name
            if dst.is_file() and _sha256(dst) == f["sha256"]:
                done.append((r["id"], name, "unchanged"))
                continue
            if _sha256(f["path"]) != f["sha256"]:
                raise ExportError(f"job {r['id']}: {f['path']} no longer matches the SHA-256 recorded when it succeeded")
            out_dir.mkdir(parents=True, exist_ok=True)
            tmp = out_dir / f".{name}.export-{os.getpid()}"
            try:
                shutil.copyfile(f["path"], tmp)
                if _sha256(tmp) != f["sha256"]:
                    raise ExportError(f"job {r['id']}: the copy of {name} does not match its SHA-256")
                os.replace(tmp, dst)
            finally:
                if tmp.exists():
                    tmp.unlink()
            done.append((r["id"], name, "exported"))
    return done
