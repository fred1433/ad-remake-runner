"""Voice, motion clips and the final edit. Local only: Kokoro-82M, Pillow, ffmpeg.

Every subprocess runs with a deadline and is stopped early when the worker
loses its lease (the `abort` event): a frozen ffmpeg or voice process cannot
hold a job for ever.
"""
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
FONTS = ROOT / "media" / "fonts"
SR = 24000
W, H = 1080, 1920

VOICE_TIMEOUT_S = 1200   # the first run downloads Kokoro-82M (about 330 MB)
CLIP_TIMEOUT_S = 300
EDIT_TIMEOUT_S = 900
FFMPEG_TIMEOUT_S = 120


class Aborted(Exception):
    """The worker lost its lease: the subprocess was stopped and nothing is published."""


def run(cmd, timeout, abort=None, capture=False):
    """subprocess.run with a deadline. Raises TimeoutError (worth a retry) when the
    process is still running after `timeout` seconds, Aborted when `abort` is set."""
    name = Path(str(cmd[0])).name
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                         stderr=subprocess.PIPE, text=True)
    end = time.monotonic() + timeout
    while True:
        try:
            out, err = p.communicate(timeout=0.2)
            break
        except subprocess.TimeoutExpired:
            why = "abort" if abort is not None and abort.is_set() else "timeout" if time.monotonic() > end else None
            if why:
                p.kill()
                p.communicate()
                if why == "abort":
                    raise Aborted(f"{name} stopped: the worker lost its lease")
                raise TimeoutError(f"{name} still running after {timeout:g} s; stopped")
    if p.returncode != 0:
        raise RuntimeError(f"{name} exited with {p.returncode}: {(err or '').strip()[-300:]}")
    return out, err


class Watch:
    """Kills streaming processes that run past their deadline or outlive the lease."""

    def __init__(self, procs, timeout, abort=None):
        self.procs, self.timeout, self.abort, self.why = procs, timeout, abort, None
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        end = time.monotonic() + self.timeout
        while not self._stop.wait(0.2):
            if self.abort is not None and self.abort.is_set():
                self.why = "abort"
            elif time.monotonic() > end:
                self.why = "timeout"
            if self.why:
                for p in self.procs:
                    p.kill()
                return

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join()
        return False

    def check(self, what):
        if self.why == "abort":
            raise Aborted(f"{what} stopped: the worker lost its lease")
        if self.why == "timeout":
            raise TimeoutError(f"{what} still running after {self.timeout:g} s; stopped")


def atomic_write(path, write):
    """write(tmp_path) then rename: a crash leaves the old file or none, never half a file."""
    path = Path(path)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        write(tmp)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()

# ------------------------------------------------------------------ voice
_PIPE = None


def _say(text, cfg):
    """Forces the pronunciation of words Kokoro would guess wrong, e.g. the brand name."""
    for word, phonemes in cfg.get("lexicon", {}).items():
        text = re.sub(rf"\b{re.escape(word)}\b", f"[{word}]({phonemes})", text)
    return text


def _kokoro(text, cfg):
    global _PIPE
    if _PIPE is None:
        from kokoro import KPipeline
        _PIPE = KPipeline(lang_code=cfg["lang"], repo_id=cfg["model"])
    chunks, words, offset = [], [], 0.0
    for r in _PIPE(_say(text, cfg), voice=cfg["voice"], speed=cfg["speed"]):
        a = r.audio.numpy()
        for t in r.tokens or []:
            words.append({"t": t.text, "ws": bool(t.whitespace), "s": None if t.start_ts is None else round(offset + t.start_ts, 3),
                          "e": None if t.end_ts is None else round(offset + t.end_ts, 3)})
        chunks.append(a)
        offset += len(a) / SR
    return np.concatenate(chunks), words


def _display_words(tokens, dur):
    """Kokoro tokens to caption words: punctuation sticks to the word before it."""
    out = []
    for t in tokens:
        if re.search(r"\w", t["t"]):
            out.append({"w": t["t"], "s": t["s"], "e": t["e"]})
        elif out:
            out[-1]["w"] += t["t"]
    # fill any missing time by spreading over the fragment, in proportion to length
    if any(w["s"] is None for w in out):
        total = sum(len(w["w"]) for w in out) or 1
        t = 0.0
        for w in out:
            d = dur * len(w["w"]) / total
            w["s"], w["e"], t = round(t, 3), round(t + d, 3), t + d
    return out


def _cache_key(text, cfg):
    return hashlib.sha1(json.dumps([text, cfg["voice"], cfg["speed"], cfg.get("lexicon", {})],
                                   sort_keys=True).encode()).hexdigest()[:16]


def _cached(cache_dir, key):
    """A cache entry exists only once its .json, written last, is there."""
    wav, meta = cache_dir / f"{key}.wav", cache_dir / f"{key}.json"
    return (wav, meta) if wav.exists() and meta.exists() else None


def fill_voice_cache(texts, cfg, cache_dir):
    """Synthesizes the missing fragments into the cache (run in a child process)."""
    import soundfile as sf
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    for text in texts:
        key = _cache_key(text, cfg)
        if _cached(cache_dir, key):
            continue
        audio, tokens = _kokoro(text, cfg)
        atomic_write(cache_dir / f"{key}.wav", lambda t: sf.write(t, audio, SR, format="WAV"))
        atomic_write(cache_dir / f"{key}.json", lambda t: t.write_text(json.dumps(tokens)))


def synthesize(fragments, cfg, cache_dir, abort=None, timeout=VOICE_TIMEOUT_S):
    """One Kokoro call per fragment (cached by text), laid end to end with pauses.
    Missing fragments are voiced by a child process with a deadline.
    Returns the voice track and the timeline: where each fragment and word falls."""
    import soundfile as sf
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    missing = [f["text"] for f in fragments if not _cached(cache_dir, _cache_key(f["text"], cfg))]
    if missing:
        job = cache_dir / f".request.{os.getpid()}.json"
        job.write_text(json.dumps({"texts": missing, "cfg": cfg}))
        try:
            run([sys.executable, "-m", "remake.voice", str(cache_dir), str(job)], timeout, abort)
        finally:
            job.unlink(missing_ok=True)
    lead, t, track, timeline = 0.12, 0.12, [np.zeros(int(0.12 * SR), dtype=np.float32)], []
    for i, f in enumerate(fragments):
        wav, meta = _cached(cache_dir, _cache_key(f["text"], cfg))
        audio, _ = sf.read(wav, dtype="float32")
        tokens = json.loads(meta.read_text())
        dur = len(audio) / SR
        words = [{"w": w["w"], "s": round(t + w["s"], 3), "e": round(t + w["e"], 3)} for w in _display_words(tokens, dur)]
        timeline.append({**f, "start": round(t, 3), "end": round(t + dur, 3), "words": words})
        track.append(audio.astype(np.float32))
        pause = f.get("pause", 0.24)
        nxt = fragments[i + 1] if i + 1 < len(fragments) else None
        if f["beat"] == 1 and nxt and nxt["beat"] != 1:
            pause = max(0.2, 2.95 - (t + dur))  # the opening is a 3-second slot
        if nxt is None:
            pause = 1.6  # hold the end card
        track.append(np.zeros(int(round(pause * SR)), dtype=np.float32))
        t = t + dur + pause
    audio = np.concatenate(track)
    return audio, timeline, round(len(audio) / SR, 3), lead


def beat_bounds(timeline, total):
    beats = sorted({f["beat"] for f in timeline})
    starts = {b: (0.0 if b == beats[0] else min(f["start"] for f in timeline if f["beat"] == b) - 0.04) for b in beats}
    out = {}
    for i, b in enumerate(beats):
        out[b] = (starts[b], starts[beats[i + 1]] if i + 1 < len(beats) else total)
    return out


def segments_for_beat(timeline, beat, bounds):
    """Shots of one beat: each shot runs from its fragment's first word to the next fragment."""
    ps, pe = bounds[beat]
    frs = [f for f in timeline if f["beat"] == beat]
    segs = []
    for i, f in enumerate(frs):
        s = ps if i == 0 else f["start"] - 0.04
        e = pe if i + 1 == len(frs) else frs[i + 1]["start"] - 0.04
        if segs and segs[-1]["image"] == f["image"]:
            segs[-1]["end"] = e
            continue
        segs.append({"image": f["image"], "motion": f.get("motion", "push"), "focal": f.get("focal", [0.5, 0.5]),
                     "start": round(s, 3), "end": round(e, 3)})
    return segs


# ------------------------------------------------------------------ motion
def _ease(x):
    x = min(max(x, 0.0), 1.0)
    return x * x * (3 - 2 * x)


def _ease_out(x):
    x = min(max(x, 0.0), 1.0)
    return 1 - (1 - x) ** 3


MOTIONS = {
    # zoom from, zoom to, centre from (fraction of the way to the focal point), centre to, easing
    "push": (1.0, 1.10, 0.0, 0.6, _ease),
    "push_fast": (1.0, 1.17, 0.0, 0.8, _ease_out),
    "pull": (1.12, 1.0, 0.6, 0.0, _ease),
    "pull_slow": (1.06, 1.0, 0.3, 0.0, _ease),
    "hold": (1.02, 1.05, 0.3, 0.4, _ease),
}


def _crop_box(img_w, img_h, motion, focal, x, punch):
    cw = min(img_w, img_h * 9 / 16)
    ch = cw * 16 / 9
    fx, fy = focal[0] * img_w, focal[1] * img_h
    if motion in ("fall", "rise", "drift"):
        z = 1.12
        w, h = cw / z, ch / z
        k = _ease(x)
        if motion == "drift":
            cx = (img_w / 2 - (img_w - w) * 0.18) + (img_w - w) * 0.36 * k
            cy = fy
        else:
            top, bottom = h / 2, img_h - h / 2
            a, b = (top, bottom) if motion == "fall" else (bottom, top)
            cy = a + (b - a) * (0.25 + 0.5 * k)
            cx = fx
    else:
        z0, z1, c0, c1, ease = MOTIONS[motion]
        k = ease(x)
        z = z0 + (z1 - z0) * k
        frac = c0 + (c1 - c0) * k
        cx = img_w / 2 + (fx - img_w / 2) * frac
        cy = img_h / 2 + (fy - img_h / 2) * frac
        w, h = cw / z, ch / z
    w, h = w / punch, h / punch
    cx = min(max(cx, w / 2), img_w - w / 2)
    cy = min(max(cy, h / 2), img_h - h / 2)
    return (cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2)


def render_clip(spec, out_path, timeout=CLIP_TIMEOUT_S):
    """One beat of the film: its shots in motion, frame-exact, no sound."""
    fps = spec["fps"]
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    f0 = round(spec["start"] * fps)
    p = subprocess.Popen(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "rawvideo", "-pix_fmt", "rgb24",
                          "-s", f"{W}x{H}", "-r", str(fps), "-i", "-", "-c:v", "libx264", "-preset", "veryfast",
                          "-crf", "14", "-pix_fmt", "yuv420p", "-r", str(fps), str(out_path)], stdin=subprocess.PIPE)
    frames = 0
    with Watch([p], timeout) as watch:
        try:
            for n, seg in enumerate(spec["segments"]):
                img = Image.open(seg["path"]).convert("RGB")
                a, b = round(seg["start"] * fps), round(seg["end"] * fps)
                if n == 0:
                    a = f0
                length = max(1, b - a)
                quick = (seg["end"] - seg["start"]) < 2.2 and spec.get("cut_punch", True)
                for i in range(length):
                    punch = 1.0 + (0.035 * (1 - _ease_out(i / 7)) if quick and i < 7 and seg.get("cut_in", True) else 0.0)
                    box = _crop_box(img.width, img.height, seg["motion"], seg["focal"], i / max(1, length - 1), punch)
                    p.stdin.write(img.resize((W, H), Image.BICUBIC, box=box).tobytes())
                    frames += 1
            p.stdin.close()
        except (BrokenPipeError, ValueError):
            pass
        code = p.wait()
    watch.check(f"ffmpeg rendering {out_path.name}")
    if code != 0:
        raise RuntimeError(f"ffmpeg failed rendering {out_path.name}")
    return {"path": str(out_path), "frames": frames}


# ------------------------------------------------------------------ edit
def _font(name, size):
    return ImageFont.truetype(str(FONTS / name), size)


CAP_FONT = "ArchivoBlack-Regular.ttf"
CAP_Y = 1250        # caption centre: clear of the bottom fifth that Reels and TikTok cover
CAP_MAX_W = 820     # clear of the right-hand action rail
YELLOW = (255, 222, 77)


def caption_chunks(timeline):
    chunks = []
    for fi, f in enumerate(timeline):
        cur = []
        for w in f["words"]:
            cur.append(w)
            if len(cur) == 3 or re.search(r"[,.:;!?]$", w["w"]):
                chunks.append({"words": cur, "frag": fi})
                cur = []
        if cur:
            chunks.append({"words": cur, "frag": fi})
    for c in chunks:
        c["s"] = c["words"][0]["s"]
    for i, c in enumerate(chunks):
        f = timeline[c["frag"]]
        nxt = chunks[i + 1] if i + 1 < len(chunks) else None
        if nxt and nxt["frag"] == c["frag"]:
            c["e"] = nxt["s"]
        else:
            nxt_start = timeline[c["frag"] + 1]["start"] if c["frag"] + 1 < len(timeline) else f["end"] + 1.2
            c["e"] = min(nxt_start - 0.02, f["end"] + 0.45)
    return chunks


def _draw_caption(words, active):
    font = _font(CAP_FONT, 74)
    space = font.getlength(" ")
    lines, cur, cur_w = [], [], 0.0
    for i, w in enumerate(words):
        lw = font.getlength(w["w"])
        if cur and cur_w + space + lw > CAP_MAX_W:
            lines.append(cur)
            cur, cur_w = [], 0.0
        cur.append((i, w["w"], lw))
        cur_w += (space if len(cur) > 1 else 0) + lw
    lines.append(cur)
    img = Image.new("RGBA", (W, 320), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    lh = 92
    y = (320 - lh * len(lines)) // 2
    for line in lines:
        total = sum(x[2] for x in line) + space * (len(line) - 1)
        x = (W - total) / 2
        for i, text, lw in line:
            fill = YELLOW if i == active else (255, 255, 255)
            d.text((x, y), text, font=font, fill=fill, stroke_width=9, stroke_fill=(17, 17, 17))
            x += lw + space
        y += lh
    return img


def _badge():
    font = _font(CAP_FONT, 25)
    text = "Animatic. Fictional brand."
    tw = font.getlength(text)
    img = Image.new("RGBA", (int(tw) + 40, 52), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle((0, 0, img.width - 1, 51), radius=26, fill=(20, 20, 20, 150))
    d.text((20, 11), text, font=font, fill=(255, 255, 255, 235))
    return img


CARD_Y = 128        # top of the brand card: just under the badge
CARD_INK = (38, 66, 44)


def brand_card(brand, tagline, plate=False):
    """The brand name and tagline, typeset by the edit step (the image model never draws the name).
    With `plate`, they sit on a light card, for shots with a busy top (the box at the door)."""
    f1, f2 = _font("DMSerifDisplay-Regular.ttf", 112), _font(CAP_FONT, 30)
    b1, b2 = f1.getbbox(brand), f2.getbbox(tagline)
    gap, pad = 16, (26 if plate else 0)
    h = (b1[3] - b1[1]) + gap + (b2[3] - b2[1]) + 8 + 2 * pad
    img = Image.new("RGBA", (W, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    if plate:
        w = max(f1.getlength(brand), f2.getlength(tagline)) + 2 * pad + 24
        d.rounded_rectangle(((W - w) / 2, 0, (W + w) / 2, h - 1), radius=18, fill=(244, 241, 232, 236))
    d.text(((W - f1.getlength(brand)) / 2, pad - b1[1]), brand, font=f1, fill=CARD_INK)
    y2 = pad + (b1[3] - b1[1]) + gap - b2[1]
    d.text(((W - f2.getlength(tagline)) / 2, y2), tagline, font=f2, fill=CARD_INK)
    return img


def _fade(card, a):
    if a >= 1:
        return card
    out = card.copy()
    out.putalpha(out.getchannel("A").point(lambda v: int(v * max(0.0, a))))
    return out


def _loudnorm(src, dst, target=-14.0, abort=None):
    """Two-pass EBU R128 normalisation of the voice track."""
    _, first = run(["ffmpeg", "-hide_banner", "-i", str(src), "-af",
                    f"loudnorm=I={target}:TP=-1.5:LRA=11:print_format=json", "-f", "null", "-"],
                   FFMPEG_TIMEOUT_S, abort)
    m = json.loads(first[first.rindex("{"):first.rindex("}") + 1])
    flt = (f"loudnorm=I={target}:TP=-1.5:LRA=11:measured_I={m['input_i']}:measured_TP={m['input_tp']}"
           f":measured_LRA={m['input_lra']}:measured_thresh={m['input_thresh']}:offset={m['target_offset']}:linear=true")
    run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", str(src), "-af", flt, "-ar", "48000",
         "-c:a", "pcm_s16le", str(dst)], FFMPEG_TIMEOUT_S, abort)


def measure_lufs(path, abort=None):
    _, err = run(["ffmpeg", "-hide_banner", "-i", str(path), "-af", "loudnorm=print_format=json", "-vn",
                  "-f", "null", "-"], FFMPEG_TIMEOUT_S, abort)
    m = json.loads(err[err.rindex("{"):err.rindex("}") + 1])
    return float(m["input_i"])


def assemble(clips, timeline, voice_wav, total, out_path, brand, tagline, fps=30, abort=None,
             timeout=EDIT_TIMEOUT_S):
    """The final film: clips end to end, normalised voice, captions, badge, brand cards.
    `out_path` is a private path (named after the job and fence); publishing is the runner's job."""
    out_path = Path(out_path)
    work = out_path.parent / (out_path.stem + "_work")
    work.mkdir(parents=True, exist_ok=True)
    lst = work / "clips.txt"
    lst.write_text("".join(f"file '{Path(c).resolve()}'\n" for c in clips))
    concat = work / "concat.mp4"
    run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "concat", "-safe", "0", "-i", str(lst),
         "-c", "copy", str(concat)], FFMPEG_TIMEOUT_S, abort)
    voice_norm = work / "voice_norm.wav"
    _loudnorm(voice_wav, voice_norm, abort=abort)

    chunks = caption_chunks(timeline)
    cache = {}
    badge = _badge()
    card, name_card = brand_card(brand, tagline), brand_card(brand, tagline, plate=True)
    end_t = min((f["start"] for f in timeline if f.get("endcard")), default=None)
    names = [(f["start"], f["end"] + 0.2) for f in timeline if f.get("namecard")]
    n_frames = round(total * fps)

    dec = subprocess.Popen(["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", str(concat), "-f", "rawvideo",
                            "-pix_fmt", "rgb24", "-"], stdout=subprocess.PIPE)
    enc = subprocess.Popen(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "rawvideo", "-pix_fmt", "rgb24",
                            "-s", f"{W}x{H}", "-r", str(fps), "-i", "-", "-i", str(voice_norm),
                            "-filter_complex", "[1:a]apad[a]", "-map", "0:v", "-map", "[a]",
                            "-c:v", "libx264", "-preset", "medium", "-crf", "20", "-profile:v", "high", "-level", "4.2",
                            "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "160k", "-ar", "48000",
                            "-t", f"{n_frames / fps:.3f}", "-movflags", "+faststart", str(out_path)],
                           stdin=subprocess.PIPE)
    size = W * H * 3
    with Watch([dec, enc], timeout, abort) as watch:
        try:
            for n in range(n_frames):
                raw = dec.stdout.read(size)
                if len(raw) < size:
                    break
                t = n / fps
                frame = Image.frombytes("RGB", (W, H), raw).convert("RGBA")
                frame.alpha_composite(badge, (44, 64))
                if end_t is not None and t >= end_t:
                    frame.alpha_composite(_fade(card, min(1.0, (t - end_t) / 0.35)), (0, CARD_Y))
                for s0, s1 in names:
                    if s0 <= t < s1:
                        frame.alpha_composite(_fade(name_card, min(1.0, (t - s0) / 0.25, (s1 - t) / 0.25)),
                                              (0, CARD_Y + 60))
                for ci, c in enumerate(chunks):
                    if c["s"] <= t < c["e"]:
                        active = max([i for i, w in enumerate(c["words"]) if w["s"] <= t] or [0])
                        key = (ci, active)
                        if key not in cache:
                            cache[key] = _draw_caption(c["words"], active)
                        frame.alpha_composite(cache[key], (0, CAP_Y - 160))
                        break
                enc.stdin.write(frame.convert("RGB").tobytes())
            enc.stdin.close()
        except (BrokenPipeError, ValueError):
            pass
        dec.stdout.close()
        dec.wait()
        code = enc.wait()
    watch.check("the final encode")
    if code != 0:
        raise RuntimeError("ffmpeg failed encoding the final film")
    poster = out_path.with_suffix(".jpg")
    run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", "1.4", "-i", str(out_path),
         "-frames:v", "1", "-q:v", "3", str(poster)], FFMPEG_TIMEOUT_S, abort)
    return {"film": str(out_path), "poster": str(poster), "duration_s": round(n_frames / fps, 3),
            "frames": n_frames, "lufs": round(measure_lufs(out_path, abort), 1), "captions": len(chunks)}
