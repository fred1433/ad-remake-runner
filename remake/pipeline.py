"""The remake flow as runner steps.

Prepared before the run: the source breakdown, the script (three openings, one
body), Gus's character reference and the storyboard stills (see media/prompts).
The first steps load and check them. Computed by the run: the voice (Kokoro-82M,
local), one motion clip per beat (through the simulated clip provider) and the
edit (captions, brand cards, loudness, final encode).

A beat is one of the six narrative units of the ad; a shot is one image inside it.
"""
import hashlib
import json
from pathlib import Path

from . import media

ROOT = Path(__file__).resolve().parent.parent
FILM = ROOT / "film"
MEDIA = ROOT / "media"
N_BEATS = 6
STEPS = ["breakdown", "script", "voice", "characters", "storyboard"] + [f"clip:{i:02d}" for i in range(1, N_BEATS + 1)] + ["edit"]
STEP_LABELS = {"breakdown": "Validate breakdown", "script": "Load script", "voice": "Voice",
               "characters": "Load character reference", "storyboard": "Validate storyboard", "edit": "Edit"}
COST = 60  # credits held per film


class StepError(Exception):
    pass


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class FilmPipeline:
    abort = None  # set by the worker: stops subprocesses when its lease is lost

    def __init__(self, build_dir):
        self.build = Path(build_dir)
        self.script = json.loads((FILM / "script.json").read_text())

    def workdir(self, job):
        """Files of one worker on one job: build/job<id>/f<fence>/. Never shared, never published from here."""
        d = self.build / f"job{job['id']}" / f"f{job['fence']}"
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ---------------------------------------------------------------- steps
    def run(self, job, step, outputs):
        return getattr(self, "step_" + step)(job, outputs)

    def step_breakdown(self, job, outputs):
        """Validate breakdown: the prepared breakdown's times are in order and every beat has a source moment."""
        b = json.loads((FILM / "breakdown.json").read_text())
        times = [m["t"] for m in b["moments"]]
        if times != sorted(times) or not all(0 <= t <= b["source"]["duration_s"] for t in times):
            raise StepError("breakdown times must be increasing and inside the source")
        beats = {m["beat"] for m in b["moments"]}
        if beats != set(range(1, N_BEATS + 1)):
            raise StepError(f"every beat needs a source moment, got {sorted(beats)}")
        return {"moments": len(b["moments"]), "source": b["source"]["youtube_id"]}

    def step_script(self, job, outputs):
        """Load script: the prepared opening for this job, then the shared body."""
        hook = job["params"]["hook"]
        if hook not in self.script["hooks"]:
            raise StepError(f"unknown hook {hook!r}; choose one of {', '.join(self.script['hooks'])}")
        frs = [{**f, "beat": 1} for f in self.script["hooks"][hook]["fragments"]]
        for b in self.script["beats"]:
            frs += [{**f, "beat": b["id"]} for f in b.get("fragments", [])]
        return {"hook": hook, "fragments": frs, "words": sum(len(f["text"].split()) for f in frs)}

    def step_voice(self, job, outputs):
        frs = outputs["script"]["fragments"]
        audio, timeline, total, _ = media.synthesize(frs, self.script["voice"], self.build / "cache" / "voice",
                                                     abort=self.abort)
        import soundfile as sf
        wav = self.workdir(job) / "voice.wav"
        sf.write(wav, audio, media.SR)
        return {"wav": str(wav), "timeline": timeline, "total_s": total, "engine": self.script["voice"]["engine"]}

    def step_characters(self, job, outputs):
        """Load character reference: Gus's prepared reference sheet, fingerprinted."""
        ref = MEDIA / "gus_reference.png"
        if not ref.exists():
            raise StepError("media/gus_reference.png is missing")
        return {"gus": str(ref), "sha256": sha256(ref)[:16]}

    def step_storyboard(self, job, outputs):
        """Validate storyboard: every still the script needs exists, fingerprinted."""
        needed = sorted({f["image"] for f in outputs["voice"]["timeline"]})
        missing = [n for n in needed if not (MEDIA / "storyboard" / f"{n}.png").exists()]
        if missing:
            raise StepError(f"storyboard images missing: {', '.join(missing)}")
        return {"images": {n: sha256(MEDIA / 'storyboard' / f'{n}.png')[:16] for n in needed}}

    def step_edit(self, job, outputs):
        clips = [outputs[f"clip:{i:02d}"]["clip"] for i in range(1, N_BEATS + 1)]
        v = outputs["voice"]
        hook = job["params"]["hook"]
        private = self.workdir(job) / f"remake_hook{hook}.mp4"
        res = media.assemble(clips, v["timeline"], v["wav"], v["total_s"], private, self.script["brand"],
                             self.script["tagline"], self.script["fps"], abort=self.abort)
        film, poster = ROOT / job["params"]["out"], (ROOT / job["params"]["out"]).with_suffix(".jpg")
        # published by the runner in the transaction that records this step
        return {**res, "film": str(film), "poster": str(poster), "rendered": res["film"],
                "publish": {str(film): res["film"], str(poster): res["poster"]}}

    # ---------------------------------------------------------------- clips
    def clip_spec(self, job, step, outputs):
        beat = int(step.split(":")[1])
        v = outputs["voice"]
        bounds = media.beat_bounds(v["timeline"], v["total_s"])
        segs = media.segments_for_beat(v["timeline"], beat, bounds)
        for s in segs:
            s["path"] = str(MEDIA / "storyboard" / f"{s['image']}.png")
        return {"job": job["id"], "beat": beat, "start": bounds[beat][0], "end": bounds[beat][1],
                "fps": self.script["fps"], "segments": segs}

    def render(self, spec, request_id):
        """The provider's work (simulated): one file per request, so two requests never share a file."""
        out = self.build / f"job{spec['job']}" / "provider" / f"clip_{spec['beat']:02d}_{request_id}.mp4"
        return media.render_clip(spec, out)["path"]
