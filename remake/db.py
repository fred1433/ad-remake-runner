"""Postgres schema and a throwaway local cluster for runs and tests."""
import os
import shutil
import subprocess
import tempfile
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

SCHEMA = """
create table accounts (
  id text primary key,
  name text not null
);

create table jobs (
  id bigserial primary key,
  account_id text not null references accounts(id),
  kind text not null,
  params jsonb not null,
  status text not null default 'queued'
    check (status in ('queued','running','succeeded','failed','cancelled','needs_review')),
  lease_owner text,
  lease_expires_at timestamptz,
  fence bigint not null default 0,
  cancel_requested boolean not null default false,
  error text,
  output jsonb,
  created_at timestamptz not null default clock_timestamp(),
  updated_at timestamptz not null default clock_timestamp()
);

create table steps (
  job_id bigint not null references jobs(id),
  name text not null,
  seq int not null,
  status text not null default 'pending'
    check (status in ('pending','done','failed','waiting_provider','outcome_unknown')),
  attempts int not null default 0,
  next_attempt_at timestamptz,
  output jsonb,
  last_error text,
  primary key (job_id, name)
);

-- One row per submission to the clip provider. The row is written BEFORE the
-- call, so a crash between "provider accepted" and "we stored its id" leaves a
-- trace instead of a silent resubmission.
create table provider_calls (
  client_key text primary key,
  job_id bigint not null references jobs(id),
  step text not null,
  attempt int not null,
  request_id text,
  status text not null check (status in ('intent','submitted','done','failed','rejected','unknown')),
  created_at timestamptz not null default clock_timestamp(),
  updated_at timestamptz not null default clock_timestamp()
);

-- Credits held for a job: consumed once on success, released on failure or cancel.
create table credit_holds (
  job_id bigint primary key references jobs(id),
  account_id text not null references accounts(id),
  amount int not null check (amount > 0),
  status text not null check (status in ('held','consumed','released')),
  created_at timestamptz not null default clock_timestamp(),
  settled_at timestamptz
);

-- Every balance change. The unique key is what makes a replay a no-op.
create table ledger (
  id bigserial primary key,
  account_id text not null references accounts(id),
  job_id bigint references jobs(id),
  kind text not null check (kind in ('purchase','charge')),
  amount int not null,
  idempotency_key text not null unique,
  created_at timestamptz not null default clock_timestamp()
);

create table stripe_events (
  event_id text primary key,
  type text not null,
  payment_ref text,
  received_at timestamptz not null default clock_timestamp(),
  effect text not null
);

create table events (
  id bigserial primary key,
  job_id bigint,
  at timestamptz not null default clock_timestamp(),
  worker text,
  kind text not null,
  step text,
  detail jsonb not null default '{}'
);

-- Stand-in for the clip provider's own records (an external service in production).
create schema sim_provider;
create table sim_provider.requests (
  request_id text primary key,
  client_key text not null unique,
  step text not null,
  spec jsonb not null,
  fault text,
  state text not null default 'queued',
  polls int not null default 0,
  result text,
  error text,
  created_at timestamptz not null default clock_timestamp()
);
"""


def connect(dsn):
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def reset(dsn):
    with connect(dsn) as c:
        c.execute("drop schema if exists sim_provider cascade")
        c.execute("drop schema if exists public cascade")
        c.execute("create schema public")
        c.execute(SCHEMA)


@contextmanager
def local_postgres():
    """Yields a DSN. Uses DATABASE_URL when set, else starts a private cluster
    that listens on a Unix socket only (no TCP port) and deletes it afterwards."""
    if os.environ.get("DATABASE_URL"):
        yield os.environ["DATABASE_URL"]
        return
    for tool in ("initdb", "pg_ctl"):
        if not shutil.which(tool):
            raise SystemExit(f"{tool} not found: install PostgreSQL 14+ or set DATABASE_URL")
    with tempfile.TemporaryDirectory(prefix="rmk") as tmp:
        data, sock = os.path.join(tmp, "data"), tmp
        subprocess.run(["initdb", "-D", data, "-U", "remake", "--auth=trust", "-E", "UTF8", "--no-locale"],
                       check=True, capture_output=True)
        opts = f"-c listen_addresses='' -k {sock} -c fsync=off -c synchronous_commit=off"
        subprocess.run(["pg_ctl", "-D", data, "-l", os.path.join(tmp, "pg.log"), "-o", opts, "-w", "start"],
                       check=True, capture_output=True)
        try:
            yield f"host={sock} user=remake dbname=postgres"
        finally:
            subprocess.run(["pg_ctl", "-D", data, "-m", "fast", "-w", "stop"], capture_output=True)
