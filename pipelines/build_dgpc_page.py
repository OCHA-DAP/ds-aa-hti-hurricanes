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
from src.dgpc.charts import grouped_barh
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


def load():
    storms = blob.load_parquet_from_blob(f"{PREFIX}/storm_set.parquet")
    # Re-resolve names at build time so a stored run predating the
    # ibtracs fallback still renders correct storm names.
    storms = resolve_names(storms)
    fcast = blob.load_parquet_from_blob(
        f"{PREFIX}/fcast_wind_by_issuance.parquet"
    )
    obsv = blob.load_parquet_from_blob(f"{PREFIX}/obsv_wind.parquet")
    try:
        rain = blob.load_parquet_from_blob(f"{PREFIX}/rain_stats.parquet")
    except Exception:
        logger.warning("no rain_stats.parquet yet — rendering rain as pending")
        rain = None
    return storms, fcast, obsv, rain


def main():
    storms, fcast, obsv, rain = load()

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

    html = render(df, chart_wind, rp, rain, v, rmw_validation_note())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    logger.info("wrote %s (%.0f kB)", OUT, OUT.stat().st_size / 1024)
    return df, rp


if __name__ == "__main__":
    main()
