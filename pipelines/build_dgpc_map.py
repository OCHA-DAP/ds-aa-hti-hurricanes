"""Build docs/carte-avis.html and, unless --html-only, its data files.

The data (``docs/assets/carte/``) needs the storms DB for tracks, wind
radii and observed swaths, and takes about 15 s per storm.

Usage: uv run python pipelines/build_dgpc_map.py [--html-only] [--storm AL142016 ...]
"""

import argparse
import json
from pathlib import Path

from src.constants import TRIGGERS
from src.dgpc.activations import join_deck, storm_label
from src.dgpc.dept_page import PROPOSED_N
from src.dgpc.map_page import render
from src.dgpc.pathways import PREFIX
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

OUT = Path("docs/carte-avis.html")
DATA_DIR = Path("docs/assets/carte")


def storm_table():
    """Per-storm pathway values joined to the deck's record (impact, CERF,
    activation) - the same join the department page uses."""
    storms = blob.load_parquet_from_blob(f"{PREFIX}/storm_set.parquet")
    tbl, _ = join_deck(storms)
    tbl["label"] = tbl.apply(
        lambda r: (
            r["label"]
            if isinstance(r.get("label"), str) and r["label"]
            else storm_label(r)
        ),
        axis=1,
    )
    pdf = blob.load_parquet_from_blob(f"{PREFIX}/pathways.parquet").merge(
        tbl[["atcf_id", "label", "cerf", "pop_affected_n"]],
        on="atcf_id",
        how="left",
    )
    pdf = pdf.sort_values("season").reset_index(drop=True)
    deck_trig = (
        tbl[tbl["in_deck"] & tbl["atcf_id"].notna()]
        .set_index("atcf_id")["triggered_hit"]
        .astype(bool)
        .to_dict()
    )
    return pdf, deck_trig


def main(html_only=False, only=None):
    if not html_only:
        from src.dgpc import map_data

        map_data.write(DATA_DIR, only=set(only) if only else None)
    index = json.loads((DATA_DIR / "index.json").read_text(encoding="utf-8"))
    pdf, deck_trig = storm_table()
    meta = {
        "fcast_rain_mm": TRIGGERS["mobilisation"]["rain_mm"],
        "obsv_rain_mm": TRIGGERS["obsv"]["rain_mm"],
        "n_depts": PROPOSED_N,
    }
    OUT.write_text(
        render(len(index["storms"]), pdf, meta, deck_trig), encoding="utf-8"
    )
    logger.info("wrote %s (%.0f kB)", OUT, OUT.stat().st_size / 1024)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--html-only", action="store_true")
    ap.add_argument("--storm", nargs="*", default=None)
    a = ap.parse_args()
    main(html_only=a.html_only, only=a.storm)
