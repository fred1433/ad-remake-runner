"""Child process for the voice step: python -m remake.voice CACHE_DIR REQUEST_JSON.

Run by media.synthesize with a deadline, so a frozen synthesis can be killed
without killing the worker. Each fragment lands in the cache by atomic rename."""
import json
import sys
from pathlib import Path

from . import media

if __name__ == "__main__":
    cache_dir, request = Path(sys.argv[1]), json.loads(Path(sys.argv[2]).read_text())
    media.fill_voice_cache(request["texts"], request["cfg"], cache_dir)
