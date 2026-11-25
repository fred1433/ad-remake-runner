# ad-remake-runner

A job runner that turns a prepared script, a voice and a storyboard into a 9:16 film, and keeps going when a step fails halfway. The film it makes here is a 41-second **animatic** for Mossbell, a **fictional** self-watering planter brand, remade from the structure of a public ad (see `film/breakdown.json`). It has six **beats** (the narrative units of the ad); each beat is one or more **shots** (stills put in motion).

Page with the film, its three openings and the recorded run: https://theaipipe.com/demos/kell-remake/

## What was prepared, what runs, what is simulated

| | |
|---|---|
| Prepared before the run | The source breakdown (`film/breakdown.json`, written by hand), the script with its three openings (`film/script.json`), Gus's character reference (`media/gus_reference.png`) and the 17 storyboard stills (`media/storyboard/`), generated with gpt-image on a ChatGPT subscription. Prompts in `media/prompts/`. The first four steps load and check these: **Validate breakdown, Load script, Load character reference, Validate storyboard**. |
| Runs here | Postgres job queue with leases and fencing, retries, deadlines, credit holds and charges, Stripe event handling, the Kokoro-82M voice (local), one motion clip per beat (Pillow), the edit: captions, brand cards, loudness normalisation and the final encode (ffmpeg). |
| Simulated | The clip provider (`remake/provider.py`): Seedance, Kling or Veo are **not wired in**; this is the step where they would plug in. Its "remote" work is our local motion renderer. Stripe: events are built and signed locally in Stripe's format; no Stripe account is called. |

No paid API is called by anything in this repository. The brand name is typeset by the edit step; no storyboard still carries it.

## Run it

Needs Python 3.12, PostgreSQL 14+ binaries on the PATH (`initdb`, `pg_ctl`; a private cluster is started on a Unix socket and deleted afterwards, or set `DATABASE_URL`), and ffmpeg. The first run downloads Kokoro-82M (about 330 MB) from Hugging Face.

```
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m remake film              # the three openings, with the recorded run's incidents
.venv/bin/python -m remake film --clean      # same films, no incident
.venv/bin/python -m remake film --hooks A    # one opening
.venv/bin/python -m pytest -q                # the runner's properties, against a real Postgres
```

Keep the clone at a short path: Kokoro loads espeak-ng as its fallback for words outside its dictionary, and espeak-ng cannot find its data when the venv path is longer than about 160 characters.

`film` writes `out/remake_hookA.mp4` (and B, C), a poster per film, and the run trace `runs/recorded_run.json` (`runs/clean_run.json` with `--clean`; a run of some openings only writes its own trace, e.g. `runs/recorded_run_A.json`). The incidents of the recorded run:

- **Opening A**: the clip provider fails the clip of beat 4. It is retried after 1 s; the other five clips are not resubmitted.
- **Opening B**: the worker process is killed with SIGKILL after the clip of beat 2. Another worker takes the job over once the 6 s lease has run out and starts at the clip of beat 3.
- **Opening C**: the provider answers the clip of beat 5 late. The same request is polled again; a slow answer is not a failure.
- **Credits**: one purchase is reported by two Stripe event types, and the first event is delivered twice at the same moment. The account is credited once.

## Invariants

**Who writes what.** Every writer of a job takes its row lock first.

- `create_job` inserts the job (`queued`) and its credit hold in one transaction.
- `claim` (any worker) moves a job to `running`, bumps its `fence` and sets the lease. It takes a `queued` job, or a `running` job whose lease ran out and that has no cancel pending. Before claiming, it settles two kinds of abandoned job: a cancelled job whose worker's lease ran out (to `cancelled`) and a job past its deadline (to `failed`, live lease or not).
- The lease holder writes its job's steps, provider calls and status only inside `owned`: row lock, then fence and `running` status read again. Its heartbeat writes only `lease_expires_at` and `lease_renewed_at`, and only while the fence and the status still match.
- `cancel_job` takes the same row lock. It settles a `queued`, `needs_review` or lease-expired job, or sets `cancel_requested` for the live worker.
- The Stripe handler writes only `stripe_events` and the purchase lines of `ledger`.
- The export and the page read committed rows.

**What reopens what.** One transition only: a `running` job whose lease ran out is claimed by another worker, with a new fence, and resumes at its first unfinished step. `succeeded`, `failed` and `cancelled` are final. `needs_review` is never picked up again; `cancel_job` settles it.

- **I1. Nothing waits without a deadline, and no job stays `running` past its own.** Every provider call (submission, lookup by client key, status, cancel) returns or is abandoned after `status_timeout_s`. Every subprocess (voice, clip render, ffmpeg) is killed past its deadline. Every database connection has a connect timeout and a statement timeout. A submission that times out is not taken as lost: its row stays `intent` under the same client key, and the next attempt asks the provider for that key, then resends with it, so a late arrival is deduplicated. Limit: a submission that reaches the provider after its job has failed is left there, its row marked `abandoned`; with a real provider it would be cancelled by client key. A job's deadline (`job_deadline_s`, 3600 s in `python -m remake film`) is fixed at its first claim. Past it, its worker fails the job at its next step, and any other worker's claim fails it even while its lease is live. Tests: `test_a_submission_that_never_returns_is_bounded_and_sent_once`, `test_a_job_never_stays_running_past_its_deadline`, `test_a_job_past_its_deadline_is_failed_by_any_other_worker`.
- **I2. Every step, local or provider, has an attempt ceiling** (`max_attempts`, 4). `steps.attempts` counts tries: a run of a local step, a clip submission, a resend or lookup of a submission whose answer never came. It is checked before each try and after each failure. `steps.leases` counts the leases (fences) that picked the step up. It is checked before any work, so a step that kills its worker every time stops after four workers. Past either ceiling the job leaves `running` as `failed`, with the step and the reason in `error`. Settlement rule: the hold is released, nothing is charged, open provider calls are marked `abandoned`. One exit keeps the credits held: `needs_review`, when an answer was lost by a provider that cannot be asked by client key; `cancel_job` releases them once a person has checked. Tests: `test_a_step_that_kills_its_worker_every_time_fails_the_job` (a local step and a clip), `test_a_submission_that_always_times_out_fails_after_max_attempts`.
- **I3. A film is published only by a job that ends `succeeded` and charged.** The edit step renders into the worker's fenced folder and records the files to publish. `_succeed` takes the row lock, reads `cancel_requested` again, turns the hold into the charge, marks the job `succeeded`, renames the files into `out/` and commits, all in one transaction. A cancel recorded before that lock wins: the job ends `cancelled`, nothing is charged, nothing is published. If the commit fails, the renamed files are removed again. Limit: if the commit went through but its answer was lost, the job reads `succeeded` without its files in `out/`; the rendered copy stays in `build/job<id>/f<fence>/`. Test: `test_a_cancel_during_the_final_edit_publishes_nothing`.
- **I4. A worker that lost its lease stops before its next write.** The lease counts as lost when a renewal finds the job is no longer its own, when a renewal fails, or when no renewal has succeeded for two thirds of the lease (a heartbeat frozen without an error). A watchdog thread checks that age ten times per lease and stops the worker's subprocesses. `owned`, used before each step, each write and the publication, checks it again, then the fence in the database; so does the moment right before each provider submission. Test: `test_a_frozen_heartbeat_stops_the_worker_before_anyone_takes_over`.
- **I5. Handling a verified Stripe event never raises.** Credits must be ASCII digits, from 1 to 1,000,000 ("²", "١٢", "99999999999", "1e3" are refused). Metadata, account and payment reference must be strings. Anything else is recorded with its reason and answered 200. `verify` is the step that raises, on a bad or stale signature. Test: `test_a_malformed_credit_purchase_is_recorded_not_raised`.

## The rules the runner keeps

Each rule names the tests that hold it (`tests/`).

- **Lease and fencing.** Claiming a job sets a lease and bumps the job's fence number. Every write re-reads the job row under lock and checks the fence: a worker that stalled past its lease cannot write once another has taken over (`test_a_stalled_worker_cannot_write_after_takeover`, `test_stale_fence_is_refused_at_the_database`, `test_two_workers_never_claim_the_same_job`).
- **No work without a lease.** If the heartbeat cannot renew the lease (connection lost, job taken), or has not renewed it for two thirds of its length without any error, the worker stops at once and stops its subprocesses, instead of working on until it is fenced (I4; `test_a_lost_heartbeat_stops_the_worker_before_anyone_takes_over`, `test_a_frozen_heartbeat_stops_the_worker_before_anyone_takes_over`).
- **Files under the fence.** A step writes into `build/job<id>/f<fence>/`. The film is published to `out/` by an atomic rename inside the transaction that charges the job and marks it succeeded (I3), so a worker that lost the job, or a job cancelled before that transaction, can only leave unpublished files behind (`test_a_late_worker_can_only_leave_unpublished_files`, `test_a_cancel_during_the_final_edit_publishes_nothing`). Voice cache entries are written whole or not at all (`test_voice_cache_entries_are_written_whole_or_not_at_all`).
- **Resume, not restart.** Finished steps are kept. After a failure only the failed clip is redone (`test_only_the_failed_clip_is_redone`, `test_killed_worker_job_is_taken_over_after_the_lease`).
- **Retries.** Provider failures and timeouts are retried after 1, 2, then 4 s (the wait doubles, capped at 8 s), four attempts in all, then the job fails with the step, the attempt count and the provider's message (`clip:03 failed after 4 attempts: provider failed clip:03: 503 ...`). The ceiling is checked before every try, local step or submission, so a crash after the last failure sends nothing more; a step that kills its worker every time stops after four workers (I2). A bug is not retried (`test_retry_ceiling_gives_a_clear_error_and_releases_the_reservation`, `test_backoff_doubles_up_to_its_cap`, `test_crash_after_the_last_failed_attempt_sends_nothing_more`, `test_a_bug_is_not_retried`, `test_a_step_that_kills_its_worker_every_time_fails_the_job`).
- **Nothing waits for ever.** A clip with no answer 300 s after submission (by the clock, even if a status call itself never returns) is cancelled at the provider and counts as a failed attempt. Submissions and lookups by client key have the same call timeout as status calls. Every subprocess (voice, ffmpeg) has a deadline and is killed past it. A job still running an hour after its first claim fails (I1) (`test_provider_that_never_answers_hits_the_deadline_and_frees_the_credits`, `test_a_status_call_that_never_returns_is_bounded_by_the_clock`, `test_a_frozen_subprocess_is_killed_and_the_job_fails_cleanly`, `test_an_aborted_subprocess_is_stopped_at_once`, `test_slow_provider_is_not_a_failure`, `test_a_submission_that_never_returns_is_bounded_and_sent_once`, `test_a_job_never_stays_running_past_its_deadline`).
- **Provider spend.** A submission is written down, with its client key, before it is sent, and the provider's answer is recorded in the same transaction as the end of the step. If the worker dies after the provider accepted the request, the next worker polls that request instead of sending a new one; if it dies after the provider finished, the result is fetched again. If the provider says it never received the request (worker killed before sending, connection dropped before acceptance), it is sent again with the same client key, which the provider deduplicates. Only when the answer was lost and the provider cannot be asked by client key does the job stop in `needs_review`, credits held, rather than risk paying twice (`test_killed_after_submit_polls_the_same_request`, `test_crash_between_the_provider_answer_and_the_step_record_sends_nothing_new`, `test_a_done_call_whose_step_was_not_recorded_is_recovered`, `test_killed_between_writing_the_intent_and_sending_is_sent_once`, `test_connection_dropped_before_the_provider_got_it_is_retried`, `test_frozen_before_sending_then_sending_late_creates_one_request`, `test_lost_answer_is_found_by_client_key`, `test_lost_answer_without_lookup_waits_for_a_person`).
- **Credits.** Creating a job holds its credits (the account row is locked, so two jobs cannot spend the same credits). Success turns the hold into one ledger charge with the key `charge:job:<id>`; failure or cancel releases the hold, and nothing is charged (`test_one_charge_with_its_ledger_line`, `test_two_jobs_cannot_spend_the_same_credits`, `test_not_enough_credits_creates_nothing`).
- **One outcome.** `cancel_job` settles a queued job, a job waiting for review, or a running job whose lease has run out, and releases its hold; a live worker stops at its next step. A cancel that arrives before success is recorded wins, and nothing is charged; a cancel after success changes nothing (`test_cancel_queued_job_releases_its_reservation`, `test_cancel_running_job_stops_at_the_next_step`, `test_cancel_that_arrives_just_before_success_wins_and_nothing_is_charged`, `test_cancel_resolves_a_job_waiting_for_review_and_releases_its_credits`, `test_cancel_of_a_job_whose_worker_died_settles_when_the_lease_runs_out`, `test_cancel_after_success_changes_nothing`).
- **Stripe.** Purchases are keyed on the payment intent, not on the event: a replay, a concurrent duplicate or two event types in either order credit once. A checkout session credits only when `payment_status` is `paid` (a delayed payment credits on `checkout.session.async_payment_succeeded`) and has a payment reference. An event without credit metadata, with a credit count that is not a whole number in ASCII digits from 1 to 1,000,000, or for an unknown account is recorded with the reason and answered without error (I5). Signatures are checked (HMAC-SHA256, 5-minute tolerance), against every `v1` value so that a secret rotation does not reject events (`tests/test_stripe.py`).

## Layout

| Path | What it is |
|---|---|
| `remake/worker.py` | Claiming, lease heartbeat, fencing, retries, deadlines, provider polling, cancel, publishing |
| `remake/credits.py` | Holds, charges, Stripe signature and event handling |
| `remake/provider.py` | The simulated clip provider and its fault injection |
| `remake/pipeline.py` | The film's steps: validate breakdown, load script, voice, load character reference, validate storyboard, six clips, edit |
| `remake/media.py` | Kokoro voice, motion clips, captions, brand cards, loudness, final encode, subprocess deadlines |
| `remake/voice.py` | The voice step's child process |
| `remake/db.py` | Schema and the throwaway local Postgres |
| `film/` | The script (three openings, one body) and the source breakdown |
| `tests/` | Runner, recovery and Stripe tests; they use a fast stand-in flow with the same step names |

Fonts: Archivo Black and DM Serif Display, SIL Open Font License (`media/fonts/`).
