"""Build docs/dgpc-departements.html from the department-level outputs.

Reads ``dept_verdicts.parquet`` (written by ``run_dgpc_dept_forecast.py``)
and the storm set, joins the deck's activation record, and renders the
storm-by-storm page.

Usage: uv run python pipelines/build_dgpc_dept_page.py
"""

from pathlib import Path


from src.dgpc.activations import join_deck, storm_label
from src.dgpc.charts import grouped_barh
from src.dgpc.dept_forecast import (
    READINGS,
    department_frequency,
    storm_counts,
)
from src.dgpc.dept_page import render
from src.dgpc.wind_analysis import resolve_names
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

PREFIX = f"{blob.PROJECT_PREFIX}/processed/dgpc"
OUT = Path("docs/dgpc-departements.html")


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

    pdf = blob.load_parquet_from_blob(f"{PREFIX}/pathways.parquet").merge(
        tbl[["atcf_id", "label"]], on="atcf_id", how="left"
    )
    deck_trig = (
        tbl[tbl["in_deck"] & tbl["atcf_id"].notna()]
        .set_index("atcf_id")["triggered_hit"]
        .astype(bool)
        .to_dict()
    )
    html = render(
        tbl,
        verdicts,
        counts,
        freq,
        chart=chart,
        notes={"deck": deck_note},
        pathways=(pdf, deck_trig),
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    logger.info("wrote %s (%.0f kB)", OUT, OUT.stat().st_size / 1024)

    return tbl


if __name__ == "__main__":
    main()
