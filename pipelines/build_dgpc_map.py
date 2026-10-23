"""Write the data behind the 2026 page's advisory-by-advisory map.

``docs/2026/assets/carte/``: an index (thresholds, department polygons,
storm list) and one file per storm. Needs the storms DB for tracks, wind
radii and observed swaths, and takes about 15 s per storm. The page
itself is rendered by ``build_page_2026.py``.

Usage: uv run python pipelines/build_dgpc_map.py [--storm AL142016 ...]
"""

import argparse
from pathlib import Path

from src.dgpc import map_data
from src.utils.logging import get_logger

logger = get_logger(__name__)

DATA_DIR = Path("docs/2026/assets/carte")


def main(only=None):
    index = map_data.write(DATA_DIR, only=set(only) if only else None)
    logger.info("wrote %s (%d storms)", DATA_DIR, len(index["storms"]))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--storm", nargs="*", default=None)
    a = ap.parse_args()
    main(only=a.storm)
