"""The remake flow as runner steps: breakdown, script, voice, characters,
storyboard, six clips, edit. Images were generated before the run (see
media/prompts); everything else is computed here."""
import hashlib
import json
from pathlib import Path

from . import media

ROOT = Path(__file__).resolve().parent.parent
FILM = ROOT / "film"
MEDIA = ROOT / "media"
N_PLANS = 6
STEPS = ["breakdown", "script", "voice", "characters", "storyboard"] + [f"clip:{i:02d}" for i in range(1, N_PLANS + 1)] + ["edit"]
COST = 60  # credits held per film


class StepError(Exception):
    pass


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class FilmPipeline:
    def __init__(self, build_dir):
        self.build = Path(build_dir)
        self.script = json.loads((FILM / "script.json").read_text())

    # ---------------------------------------------------------------- steps
    def run(self, job, step, outputs):
        return getattr(self, "step_" + step)(job, outputs)

    def step_breakdown(self, job, outputs):
        b = json.loads((FILM / "breakdown.json").read_text())
        times = [m["t"] for m in b["moments"]]
        if times != sorted(times) or not all(0 <= t <= b["source"]["duration_s"] for t in times):
            raise StepError("breakdown times must be increasing and inside the source")
        plans = {m["plan"] for m in b["moments"]}
        if plans != set(range(1, N_PLANS + 1)):
            raise StepError(f"every plan needs a source moment, got {sorted(plans)}")
        return {"moments": len(b["moments"]), "source": b["source"]["youtube_id"]}

    def step_script(self, job, outputs):
        hook = job["params"]["hook"]
        if hook not in self.script["hooks"]:
            raise StepError(f"unknown hook {hook!r}; choose one of {', '.join(self.script['hooks'])}")
        frs = [{**f, "plan": 1} for f in self.script["hooks"][hook]["fragments"]]
        for p in self.script["plans"]:
            frs += [{**f, "plan": p["id"]} for f in p.get("fragments", [])]
        return {"hook": hook, "fragments": frs, "words": sum(len(f["text"].split()) for f in frs)}

    def step_voice(self, job, outputs):
        frs = outputs["script"]["fragments"]
        audio, timeline, total, _ = media.synthesize(frs, self.script["voice"], self.build / "cache" / "voice")
        import soundfile as sf
        wav = self.build / f"job{job['id']}" / "voice.wav"
        wav.parent.mkdir(parents=True, exist_ok=True)
        sf.write(wav, audio, media.SR)
        return {"wav": str(wav), "timeline": timeline, "total_s": total, "engine": self.script["voice"]["engine"]}

    def step_characters(self, job, outputs):
        ref = MEDIA / "gus_reference.png"
        if not ref.exists():
            raise StepError("media/gus_reference.png is missing")
        return {"gus": str(ref), "sha256": sha256(ref)[:16]}

    def step_storyboard(self, job, outputs):
        needed = sorted({f["image"] for f in outputs["voice"]["timeline"]})
        missing = [n for n in needed if not (MEDIA / "storyboard" / f"{n}.png").exists()]
        if missing:
            raise StepError(f"storyboard images missing: {', '.join(missing)}")
        return {"images": {n: sha256(MEDIA / 'storyboard' / f'{n}.png')[:16] for n in needed}}

    def step_edit(self, job, outputs):
        clips = [outputs[f"clip:{i:02d}"]["clip"] for i in range(1, N_PLANS + 1)]
        v = outputs["voice"]
        return media.assemble(clips, v["timeline"], v["wav"], v["total_s"], ROOT / job["params"]["out"],
                              self.script["brand"], self.script["fps"])

    # ---------------------------------------------------------------- clips
    def clip_spec(self, job, step, outputs):
        plan = int(step.split(":")[1])
        v = outputs["voice"]
        bounds = media.plan_bounds(v["timeline"], v["total_s"])
        segs = media.segments_for_plan(v["timeline"], plan, bounds)
        for s in segs:
            s["path"] = str(MEDIA / "storyboard" / f"{s['image']}.png")
        return {"job": job["id"], "plan": plan, "start": bounds[plan][0], "end": bounds[plan][1],
                "fps": self.script["fps"], "segments": segs}

    def render(self, spec, request_id):
        out = self.build / f"job{spec['job']}" / f"clip_{spec['plan']:02d}_{request_id}.mp4"
        return media.render_clip(spec, out)["path"]
