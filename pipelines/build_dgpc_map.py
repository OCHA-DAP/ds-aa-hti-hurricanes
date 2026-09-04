"""Build docs/carte-avis.html and, unless --html-only, its data files.

The data (``docs/assets/carte/``) needs the storms DB for tracks, wind
radii and observed swaths, and takes about 15 s per storm.

Usage: uv run python pipelines/build_dgpc_map.py [--html-only] [--storm AL142016 ...]
"""

import argparse
import json
from pathlib import Path

from src.dgpc.map_page import render
from src.utils.logging import get_logger

logger = get_logger(__name__)

OUT = Path("docs/carte-avis.html")
DATA_DIR = Path("docs/assets/carte")


def main(html_only=False, only=None):
    if not html_only:
        from src.dgpc import map_data

        map_data.write(DATA_DIR, only=set(only) if only else None)
    index = json.loads((DATA_DIR / "index.json").read_text(encoding="utf-8"))
    OUT.write_text(render(len(index["storms"])), encoding="utf-8")
    logger.info("wrote %s (%.0f kB)", OUT, OUT.stat().st_size / 1024)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--html-only", action="store_true")
    ap.add_argument("--storm", nargs="*", default=None)
    a = ap.parse_args()
    main(html_only=a.html_only, only=a.storm)
