"""Build docs/dgpc-departements.html from the department-level outputs.

Reads ``dept_verdicts.parquet`` (written by ``run_dgpc_dept_forecast.py``)
and the storm set, joins the deck's activation record, and renders the
storm-by-storm page.

Usage: uv run python pipelines/build_dgpc_dept_page.py
"""

from pathlib import Path

import pandas as pd

from src.dgpc.activations import match_to_storm_set, parse_deck_activations
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


def storm_label(row):
    name = row.get("name")
    text_name = "" if name is None else str(name).strip()
    if text_name.lower() in ("", "nan", "none", "unnamed"):
        text_name = str(row["atcf_id"]).upper()
    else:
        text_name = text_name.title()
    return f"{text_name} {int(row['season'])}"


def join_deck(storms: pd.DataFrame):
    """Storm set with the deck's activation record attached.

    Returns the joined table and a note for the page about any row that
    needed a by-season match (the deck's "Unnamed (2002)").
    """
    deck = parse_deck_activations()
    m = match_to_storm_set(deck, storms)
    note = ""

    # A deck row with no name match: if exactly one storm of that season
    # is in the set, it is that storm.
    for i, r in m[m["atcf_id"].isna()].iterrows():
        cands = storms[storms["season"].astype(int) == int(r["season"])]
        if len(cands) == 1 and r["name"].casefold() == "unnamed":
            m.at[i, "atcf_id"] = cands.iloc[0]["atcf_id"]
            note += (
                f" La ligne « {r['storm']} » du registre est rapprochée de "
                f"{str(cands.iloc[0]['name']).title()} "
                f"{int(cands.iloc[0]['season'])}, seule tempête de cette "
                "saison dans le jeu."
            )

    deck_cols = [
        "atcf_id",
        "storm",
        "triggered_hit",
        "fcast_exposure_hit",
        "fcast_rain_hit",
        "dgpc_red_hurricane_warning_hit",
        "obsv_exposure_hit",
        "obsv_rain_hit",
        "cerf",
        "pop_affected_n",
    ]
    matched = m[m["atcf_id"].notna()][deck_cols]
    tbl = storms.merge(matched, on="atcf_id", how="left")
    tbl["in_deck"] = tbl["storm"].notna()
    tbl["triggered_hit"] = tbl["triggered_hit"].where(tbl["in_deck"], False)

    # Deck rows outside the storm set (Ivan 2004): keep, marked n/d.
    extra = m[m["atcf_id"].isna()].copy()
    if len(extra):
        extra["label"] = (
            extra["name"].str.title() + " " + extra["season"].astype(str)
        )
        extra["in_deck"] = True
        extra["first_near"] = pd.to_datetime(
            extra["season"].astype(str) + "-12-31"
        )
        tbl = pd.concat(
            [tbl, extra[[c for c in extra if c in tbl or c == "label"]]],
            ignore_index=True,
        )
    return tbl, note.strip()


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

    html = render(
        tbl,
        verdicts,
        counts,
        freq,
        chart=chart,
        notes={"deck": deck_note},
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    logger.info("wrote %s (%.0f kB)", OUT, OUT.stat().st_size / 1024)
    return tbl


if __name__ == "__main__":
    main()
