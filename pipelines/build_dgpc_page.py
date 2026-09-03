"""Build docs/dgpc-alertes.html from the DGPC analysis outputs.

Reads the parquet written by ``run_dgpc_wind.py`` (and ``run_dgpc_rain.py``
once Earthdata credentials allow it) and renders the published French page.

Usage: uv run python pipelines/build_dgpc_page.py
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.datasources import storms_db as sdb
from src.dgpc import constants as dc
from src.dgpc.activations import match_to_storm_set, parse_deck_activations
from src.dgpc.charts import grouped_barh
from src.dgpc.dept_forecast import department_frequency, storm_counts
from src.dgpc.page import fr_num, render
from src.dgpc.results import (
    rp_table,
    summarise_wind_forecast,
    summarise_wind_observed,
)
from src.dgpc.wind_analysis import resolve_names
from src.dgpc.windfield import NM_TO_KM, rmw_climo_km
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

PREFIX = f"{blob.PROJECT_PREFIX}/processed/dgpc"
OUT = Path("docs/dgpc-alertes.html")


def storm_label(row):
    name = row.get("name")
    text_name = "" if name is None else str(name).strip()
    if text_name.lower() in ("", "nan", "none", "unnamed"):
        text_name = str(row["atcf_id"]).upper()
    else:
        text_name = text_name.title()
    return f"{text_name} {int(row['season'])}"


def rmw_validation_note():
    """Compare the climatological Rmw with IBTrACS at hurricane intensity."""
    q = """
    SELECT wind_speed, max_wind_radius, ST_Y(geometry) lat
    FROM storms.ibtracs_tracks_geo
    WHERE provider = 'hurdat_atl' AND max_wind_radius > 0
      AND wind_speed >= :thr
      AND date_part('year', valid_time) BETWEEN :s AND :e
    """
    try:
        d = pd.read_sql(
            text(q),
            sdb.get_engine(),
            params={
                "thr": float(dc.RED_WIND_KT),
                "s": dc.SEASON_START,
                "e": dc.SEASON_END,
            },
        )
    except Exception:
        logger.warning("could not run the Rmw validation query")
        return ""
    if d.empty:
        return ""
    obs = d.max_wind_radius * NM_TO_KM
    climo = rmw_climo_km(d.wind_speed, d.lat)
    ratio = (obs / climo).median()
    return (
        f"Sur les {len(d)} points de trajectoire d’IBTrACS atteignant "
        f"108 nœuds ou plus ({dc.SEASON_START}–{dc.SEASON_END}), le rayon "
        f"observé médian est de {fr_num(obs.median(), 1)} km contre "
        f"{fr_num(climo.median(), 1)} km pour la climatologie, soit un "
        f"écart médian de {fr_num(abs(1 - ratio) * 100, 0)} % — la relation "
        f"est fiable précisément là où se joue le niveau rouge."
    )


def build_activations(df, rain, v):
    """The deck's activation record joined to the DGPC orange verdicts."""
    try:
        deck = parse_deck_activations()
    except Exception:
        logger.warning("could not parse the deck's activation table")
        return None

    acts = match_to_storm_set(deck, df)

    wind_cols = ["atcf_id", f"obsv_{v}_orange"]
    acts = acts.merge(
        df[[c for c in wind_cols if c in df]], on="atcf_id", how="left"
    )

    if rain is not None and len(rain):
        rain_cols = [
            "atcf_id",
            "national_mean_rain_orange",
            "department_max_rain_orange",
            "any_pixel_rain_orange",
        ]
        acts = acts.merge(
            rain[[c for c in rain_cols if c in rain]],
            on="atcf_id",
            how="left",
        )

    # The composite reading: wind OR the departmental rain criterion.
    wind_hit = acts.get(f"obsv_{v}_orange")
    rain_hit = acts.get("department_max_rain_orange")
    if wind_hit is not None and rain_hit is not None:
        w = wind_hit.fillna(False).astype(bool)
        r = rain_hit.fillna(False).astype(bool)
        combined = w | r
        # An OR is only "not met" when *both* halves are known not to be
        # met. Where the wind is undetermined (no best-track coverage) and
        # the rain did not fire, the honest answer is undetermined, not no.
        unknown = wind_hit.isna() & ~r
        acts["orange_combined"] = combined.where(
            acts["atcf_id"].notna() & ~unknown
        )

    return acts.sort_values(
        "pop_affected_n", ascending=False, na_position="last"
    )


def load():
    storms = blob.load_parquet_from_blob(f"{PREFIX}/storm_set.parquet")
    # Re-resolve names at build time so a stored run predating the
    # ibtracs fallback still renders correct storm names.
    try:
        storms = resolve_names(storms)
    except Exception as exc:  # noqa: BLE001
        # The stored set already carries names; a DB outage must not
        # block the page build.
        logger.warning("storms DB unreachable, using stored names: %s", exc)
    fcast = blob.load_parquet_from_blob(
        f"{PREFIX}/fcast_wind_by_issuance.parquet"
    )
    obsv = blob.load_parquet_from_blob(f"{PREFIX}/obsv_wind.parquet")
    try:
        rain = blob.load_parquet_from_blob(f"{PREFIX}/rain_stats.parquet")
    except Exception:
        logger.warning("no rain_stats.parquet yet — rendering rain as pending")
        rain = None
    try:
        # The archived reading: sustained wind, no cutoff.
        dept = blob.load_parquet_from_blob(
            f"{PREFIX}/dept_verdicts_archive.parquet"
        )
    except Exception:
        logger.warning("no dept_verdicts.parquet yet — rendering as pending")
        dept = None
    return storms, fcast, obsv, rain, dept


def main():
    storms, fcast, obsv, rain, dept = load()

    df = storms.merge(
        summarise_wind_forecast(fcast), on="atcf_id", how="left"
    ).merge(summarise_wind_observed(obsv), on="atcf_id", how="left")
    df["label"] = df.apply(storm_label, axis=1)
    df = df.sort_values("first_near").reset_index(drop=True)

    v = dc.PRIMARY_WIND_VARIANT
    df_chart = df.sort_values(f"obsv_{v}_max_kmh", ascending=False)

    chart_wind = grouped_barh(
        df_chart["label"].tolist(),
        [
            (
                "obsv",
                "Observation (trajectoire réelle)",
                df_chart[f"obsv_{v}_max_kmh"].tolist(),
            ),
            (
                "fcast",
                "Prévision (émission la plus forte du NHC)",
                df_chart[f"{v}_max_kmh"].tolist(),
            ),
        ],
        thresholds=[
            (dc.ORANGE_WIND_KMH[0], "Alerte orange — 100 km/h", "orange"),
            (dc.RED_WIND_KMH, "Alerte rouge — 200 km/h", "red"),
        ],
        x_title="Vent soutenu maximal sur terre en Haïti (km/h)",
    )

    rp = rp_table(
        df,
        {
            f"obsv_{v}_orange": "Vent ≥ 100 km/h — observation",
            f"{v}_orange": "Vent ≥ 100 km/h — prévision",
            f"obsv_{v}_red": "Vent ≥ 200 km/h — observation",
            f"{v}_red": "Vent ≥ 200 km/h — prévision",
        },
    )

    chart_rain = ""
    if rain is not None and len(rain):
        # Carry the wind table's storm labels over so both sections name
        # storms identically.
        rain = rain.drop(columns=["label"], errors="ignore").merge(
            df[["atcf_id", "label"]], on="atcf_id", how="left"
        )
        rain["label"] = rain["label"].fillna(rain["atcf_id"])
        rc = rain.sort_values("any_pixel_24h_mm", ascending=False)
        chart_rain = grouped_barh(
            rc["label"].tolist(),
            [
                (
                    "obsv",
                    "Point de grille (max.)",
                    rc["any_pixel_24h_mm"].tolist(),
                ),
                (
                    "extra",
                    "Moyenne départementale (max.)",
                    rc["department_max_24h_mm"].tolist(),
                ),
                (
                    "fcast",
                    "Moyenne nationale",
                    rc["national_mean_24h_mm"].tolist(),
                ),
            ],
            thresholds=[
                (
                    dc.ORANGE_RAIN["threshold_mm"],
                    "Alerte orange — 100 mm / 24 h",
                    "orange",
                )
            ],
            x_title="Cumul maximal de précipitations sur 24 h (mm)",
        )

    acts = build_activations(df, rain, v)

    dept_bundle = None
    if dept is not None and len(dept):
        counts = storm_counts(dept).merge(
            df[["atcf_id", "label"]], on="atcf_id", how="left"
        )
        counts["label"] = counts["label"].fillna(counts["atcf_id"])
        dept_bundle = (dept, counts, department_frequency(dept))

    html = render(
        df,
        chart_wind,
        rp,
        rain,
        v,
        rmw_validation_note(),
        chart_rain,
        acts,
        dept=dept_bundle,
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    logger.info("wrote %s (%.0f kB)", OUT, OUT.stat().st_size / 1024)
    return df, rp


if __name__ == "__main__":
    main()
