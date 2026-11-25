# ad-remake-runner

A job runner that turns a script, a voice and a storyboard into a 9:16 film, and keeps going when a step fails halfway. The film it makes here is a 41-second **animatic** for Mossbell, a **fictional** self-watering planter brand, remade from the structure of a public ad (see `film/breakdown.json`).

Page with the film, its three openings and a replay of the recorded run: https://theaipipe.com/demos/kell-remake/

## What runs, what was made before, what is simulated

| | |
|---|---|
| Made before the run | The 17 storyboard images in `media/storyboard/` and the character reference `media/gus_reference.png`, generated with gpt-image on a ChatGPT subscription. Prompts in `media/prompts/`. |
| Runs here | Postgres job queue with leases and fencing, retries, credit holds and charges, Stripe event handling, Kokoro-82M voice (local), motion clips (Pillow), edit, captions and loudness normalisation (ffmpeg). |
| Simulated | The clip provider (`remake/provider.py`): Seedance, Kling or Veo are **not wired in**; this is the step where they would plug in. Its "remote" work is our local motion renderer. Stripe events are built and signed locally in Stripe's format; no Stripe account is called. |

No paid API is called by anything in this repository.

## Run it

Needs Python 3.12, PostgreSQL 14+ binaries on the PATH (`initdb`, `pg_ctl`; a private cluster is started on a Unix socket and deleted afterwards, or set `DATABASE_URL`), and ffmpeg. The first run downloads Kokoro-82M (about 330 MB) from Hugging Face.

```
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m remake film              # the three openings, with the recorded run's incidents
.venv/bin/python -m remake film --clean      # same films, no incident
.venv/bin/python -m remake film --hooks A    # one opening
.venv/bin/python -m pytest -q                # the runner's properties, against a real Postgres
```

Keep the clone at a short path: espeak-ng, which Kokoro calls for words it does not know ("Mossbell"), cannot find its data when the venv path is longer than about 160 characters, and the run stops during the voice step.

`film` writes `out/remake_hookA.mp4` (and B, C), a poster per film, and the run trace `runs/recorded_run.json` (`runs/clean_run.json` with `--clean`). The incidents of the recorded run:

- **Opening A**: the clip provider fails plan 4. Plan 4 is retried after 1 s; plans 1 to 3, 5 and 6 are not resubmitted.
- **Opening B**: the worker process is killed with SIGKILL after plan 2. Another worker takes the job over once the 6 s lease has run out and starts at plan 3.
- **Opening C**: the provider answers plan 5 late. The same request is polled again; a slow answer is not a failure.
- **Credits**: one purchase is reported by two Stripe event types, and the first event is delivered twice at the same moment. The account is credited once.

## The rules the runner keeps

- **Lease and fencing.** Claiming a job sets a lease and bumps the job's fence number. Every write re-reads the job row under lock and checks the fence: a worker that stalled past its lease cannot write once another has taken over (`test_a_stalled_worker_cannot_write_after_takeover`).
- **Resume, not restart.** Finished steps are kept. After a failure only the failed plan is redone (`test_only_the_failed_plan_is_redone`).
- **Retries.** Provider failures are retried after 1, 2, then 4 s (the wait doubles, capped at 8 s), four attempts in all, then the job fails with the step, the attempt count and the provider's message (`clip:03 failed after 4 attempts: provider failed clip:03: 503 ...`). A bug is not retried.
- **Provider spend.** A submission is written down before it is sent. If the worker dies after the provider accepted it, the next worker polls that request instead of sending a new one. If the answer was lost and the provider cannot be asked by client key, the job stops in `needs_review` with its credits still held, rather than paying twice.
- **Credits.** Creating a job holds its credits (the account row is locked, so two jobs cannot spend the same credits). Success turns the hold into one ledger charge with the key `charge:job:<id>`; failure or cancel releases the hold, and nothing is charged.
- **Stripe.** Purchases are keyed on the payment intent, not on the event: a replay, a concurrent duplicate or the two event types in either order credit once. Signatures are checked (HMAC-SHA256, 5-minute tolerance).

## Layout

| Path | What it is |
|---|---|
| `remake/worker.py` | Claiming, lease heartbeat, fencing, retries, provider polling |
| `remake/credits.py` | Holds, charges, Stripe signature and event handling |
| `remake/provider.py` | The simulated clip provider and its fault injection |
| `remake/pipeline.py` | The film's steps: breakdown, script, voice, characters, storyboard, six clips, edit |
| `remake/media.py` | Kokoro voice, motion clips, captions, loudness, final encode |
| `remake/db.py` | Schema and the throwaway local Postgres |
| `film/` | The script (three openings, one body) and the source breakdown |
| `tests/` | Runner and Stripe tests; they use a fast stand-in flow with the same step names |

Fonts: Archivo Black and DM Serif Display, SIL Open Font License (`media/fonts/`).
