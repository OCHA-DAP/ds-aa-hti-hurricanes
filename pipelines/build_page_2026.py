"""Build docs/2026/index.html, the page for the 2026 framework version.

Reads the department-level verdicts and the per-storm and per-advisory
pathway values (``dept_verdicts.parquet``, ``pathways.parquet``,
``pathways_advisories.parquet``, all from ``run_dgpc_pathways.py``), the
storm set, the
audited NHC Hurricane Warnings (``data/``) and the deck's activation
record, and renders the page. The map data it loads in the browser
(``docs/2026/assets/carte/``) is written separately by
``build_dgpc_map.py``, which needs the storms DB.

Usage: uv run python pipelines/build_page_2026.py
"""

from pathlib import Path

from src.constants import LT_CUTOFF_HRS, TRIGGERS
from src.dgpc import constants as dc
from src.dgpc.activations import join_deck, storm_label
from src.dgpc.charts import grouped_barh
from src.dgpc.dept_forecast import READINGS, department_frequency, storm_counts
from src.dgpc.page_2026 import render
from src.dgpc.warnings import load_warnings, with_cutoff
from src.dgpc.wind_analysis import resolve_names
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

PREFIX = f"{blob.PROJECT_PREFIX}/processed/dgpc"
OUT = Path("docs/2026/index.html")


def main():
    storms = blob.load_parquet_from_blob(f"{PREFIX}/storm_set.parquet")
    try:
        # Freshen names from the storms DB when reachable; the stored set
        # already carries them, so a DB outage must not block the build.
        storms = resolve_names(storms)
    except Exception as exc:  # noqa: BLE001
        logger.warning("storms DB unreachable, using stored names: %s", exc)
    verdicts = blob.load_parquet_from_blob(f"{PREFIX}/dept_verdicts.parquet")

    counts = storm_counts(verdicts)
    freq = department_frequency(verdicts)

    tbl, deck_note = join_deck(storms)
    tbl["label"] = tbl.apply(
        lambda r: (
            r["label"]
            if isinstance(r.get("label"), str) and r["label"]
            else storm_label(r)
        ),
        axis=1,
    )
    tbl = tbl.merge(
        counts.drop(columns=["name", "season"]), on="atcf_id", how="left"
    )
    tbl = tbl.sort_values(["season", "first_near"]).reset_index(drop=True)

    counts = counts.merge(tbl[["atcf_id", "label"]], on="atcf_id", how="left")
    verdicts = verdicts.merge(
        tbl[["atcf_id", "label"]], on="atcf_id", how="left"
    )

    hit = counts[
        (counts[[f"n_dept_{k}" for k in READINGS]].fillna(0) > 0).any(axis=1)
    ].sort_values("n_dept_any_pix", ascending=False)
    chart = grouped_barh(
        hit["label"].tolist(),
        [
            ("obsv", "Vent", hit["n_dept_wind"].fillna(0).tolist()),
            (
                "extra",
                "Pluie — moyenne dép.",
                hit["n_dept_rain_mean"].fillna(0).tolist(),
            ),
            (
                "fcast",
                "Pluie — point",
                hit["n_dept_rain_pix"].fillna(0).tolist(),
            ),
        ],
        x_max=10,
        x_title="Départements en alerte orange (sur 10)",
    )

    # Per-storm pathway values with the deck's impact and CERF columns.
    pdf = blob.load_parquet_from_blob(f"{PREFIX}/pathways.parquet").merge(
        tbl[["atcf_id", "label", "cerf", "pop_affected_n"]],
        on="atcf_id",
        how="left",
    )
    pdf = pdf.sort_values("season").reset_index(drop=True)

    adv = blob.load_parquet_from_blob(f"{PREFIX}/pathways_advisories.parquet")
    warnings = with_cutoff(load_warnings(), adv, storms)
    labels = pdf.set_index("atcf_id")["label"]
    by_lead = warnings.sort_values("time_to_closest_h", ascending=False)
    hw_chart = grouped_barh(
        [labels[a] for a in by_lead["atcf_id"]],
        [
            (
                "fcast",
                "Hurricane Warning pour Haïti",
                by_lead["time_to_closest_h"].tolist(),
            )
        ],
        thresholds=[(LT_CUTOFF_HRS, "heure limite", "red")],
        x_max=72,
        unit=" h",
        decimals=None,
        x_title="Heures avant le passage prévu au plus près",
    )

    meta = {
        "fcast_rain_mm": TRIGGERS["action"]["rain_mm"],
        "obsv_rain_mm": TRIGGERS["obsv"]["rain_mm"],
        "n_depts": dc.ORANGE_N_DEPTS,
    }
    html = render(
        tbl,
        verdicts,
        counts,
        freq,
        pdf,
        warnings,
        meta,
        chart=chart,
        hw_chart=hw_chart,
        notes={"deck": deck_note},
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    logger.info("wrote %s (%.0f kB)", OUT, OUT.stat().st_size / 1024)

    return tbl


if __name__ == "__main__":
    main()
