"""Monitoring for the 2026 Haiti hurricane AA trigger.

Forecast stages (evaluated per NHC advisory, pre-cutoff only):
- Mobilisation (<=120 h): CHIRPS-GEFS 2-day rain >= 68 mm OR population
  exposed to >=64 kt forecast winds > 0
- Action (<=72 h): same conditions with the forecast capped at 72 h
Observational stage (evaluated per IMERG day):
- Réponse précoce: IMERG 2-day rain >= 57 mm OR population exposed to
  >=64 kt observed winds > 0

No trigger may fire once the forecast closest pass is under
LT_CUTOFF_HRS (48 h) away; informational emails still go out.

Track/exposure/WSP data comes from the storms DB (dev; written by
ds-storms-pipeline every 3 h). The old ds-nhc-forecast blob CSVs are no
longer used — that feed stopped updating after Oct 2025.

Rainfall attribution: rain is counted over the dates the track is (or
is forecast) within D_THRESH (230 km) of Haiti — the same attribution
used when calibrating the thresholds. This is a date-window rule, not
a trigger gate: wind exposure is computed without any distance gate.

Monitoring parquets are v2 (new schema); the v1 files are left intact
for the historical record and the Dash app.
"""

from typing import Literal

import geopandas as gpd
import pandas as pd

from src.constants import D_THRESH, LT_CUTOFF_HRS, TRIGGERS
from src.datasources import chirps_gefs, codab, imerg, storms_db
from src.monitoring import exposure
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

FCAST_STAGES = ["mobilisation", "action"]

# Only monitor advisories from here on; earlier ones belong to the v1
# system's era and would otherwise be backfilled on the first run.
MONITORING_START = pd.Timestamp("2026-08-01")


def _create_monitor_id(
    atcf_id: str, monitoring_type: str, issue_time: pd.Timestamp
) -> str:
    """Create standardized monitoring ID"""
    return (
        f"{atcf_id}_{monitoring_type}_{issue_time.isoformat().split('+')[0]}"
    )


def _should_skip_existing(
    monitor_id: str, existing_data: pd.DataFrame, clobber: bool
) -> bool:
    """Check if monitoring point already exists"""
    exists = monitor_id in existing_data["monitor_id"].unique()
    if exists and not clobber:
        logger.debug(f"Already monitored for {monitor_id}")
        return True
    elif not exists:
        if "obsv" in monitor_id:
            logger.debug(
                f"Processing observational monitoring for {monitor_id}"
            )
        else:
            logger.info(f"Processing monitoring for {monitor_id}")
    return False


def load_existing_monitoring_points(fcast_obsv: Literal["fcast", "obsv"]):
    blob_name = (
        f"{blob.PROJECT_PREFIX}/monitoring/"
        f"hti_{fcast_obsv}_monitoring_v2.parquet"
    )
    try:
        return blob.load_parquet_from_blob(blob_name)
    except Exception:
        logger.info(f"No existing v2 monitoring at {blob_name}; starting.")
        return pd.DataFrame(columns=["monitor_id"])


def _interp_track(
    gdf: gpd.GeoDataFrame, time_col: str, adm0_3857: gpd.GeoDataFrame
) -> pd.DataFrame:
    """30-min interpolated track with distance to Haiti (km)."""
    df = pd.DataFrame(
        {
            time_col: gdf[time_col],
            "longitude": gdf.geometry.x,
            "latitude": gdf.geometry.y,
            "wind_speed": gdf["wind_speed"],
        }
    ).drop_duplicates(subset=[time_col])
    df_interp = (
        df.set_index(time_col).resample("30min").interpolate().reset_index()
    )
    gdf_interp = gpd.GeoDataFrame(
        df_interp,
        geometry=gpd.points_from_xy(
            df_interp["longitude"], df_interp["latitude"]
        ),
        crs="EPSG:4326",
    ).to_crs(3857)
    gdf_interp["distance"] = (
        gdf_interp.geometry.distance(adm0_3857.iloc[0].geometry) / 1000
    )
    return gdf_interp


def _rain_in_window(gefs: pd.DataFrame, start_date, end_date):
    """Max 2-day rolling rainfall over [start_date, end_date] (dates)."""
    f = gefs[
        (gefs["valid_date"].dt.date >= start_date)
        & (gefs["valid_date"].dt.date <= end_date)
    ]
    return f["roll2_sum"].max() if not f.empty else None


def process_fcast_advisory(
    atcf_id: str,
    issue_time: pd.Timestamp,
    df_gefs_all: pd.DataFrame,
    adm0_3857: gpd.GeoDataFrame,
    name: str | None = None,
) -> dict | None:
    """Evaluate one NHC advisory against the forecast trigger stages.

    Returns the monitoring row, or None when the advisory should be
    deferred (not yet in the storms DB) or has no forecast points.
    """
    monitor_id = _create_monitor_id(atcf_id, "fcast", issue_time)
    gdf = exposure.fetch_fcast_track_points(atcf_id, issue_time)
    if gdf.empty:
        return None
    gdf_interp = _interp_track(gdf, "valid_time", adm0_3857)
    gdf_interp["leadtime"] = gdf_interp["valid_time"] - issue_time

    # closest pass
    landfall_row = gdf_interp.loc[gdf_interp["distance"].idxmin()]
    time_to_closest = landfall_row["leadtime"]
    past_cutoff = time_to_closest < pd.Timedelta(hours=LT_CUTOFF_HRS)
    gdf_dist = gdf_interp[gdf_interp["distance"] < D_THRESH]

    # CHIRPS-GEFS forecast issued most recently before the advisory
    gefs_recent_date = df_gefs_all[
        df_gefs_all["issue_time_approx"] < issue_time
    ]["issue_date"].max()
    gefs_issuetime = df_gefs_all[
        df_gefs_all["issue_date"] == gefs_recent_date
    ].copy()
    gefs_issuetime["roll2_sum"] = (
        gefs_issuetime["mean"]
        .rolling(window=2, center=True, min_periods=1)
        .sum()
    )

    # wind exposure (leadtime-capped fcastonly + cumulative obsv)
    exp_by_stage = {}
    for stage in FCAST_STAGES:
        lt_cap = TRIGGERS[stage]["lt_max_hrs"]
        exp_fcast = exposure.fcastonly_exposure(atcf_id, issue_time, lt_cap)
        if exp_fcast is None:
            logger.info(
                f"Advisory {monitor_id} not yet in storms DB; "
                "deferring to next run."
            )
            return None
        exp_by_stage[stage] = exp_fcast
    exp_obsv = exposure.obsv_exposure_at(atcf_id, issue_time)

    row = {
        "monitor_id": monitor_id,
        "atcf_id": atcf_id,
        "name": name or storms_db.fetch_storm_name(atcf_id),
        "issue_time": issue_time,
        "time_to_closest": time_to_closest,
        "closest_s": landfall_row["wind_speed"],
        "past_cutoff": past_cutoff,
        "min_dist": gdf_interp["distance"].min(),
        "exp_obsv": exp_obsv,
    }
    for stage in FCAST_STAGES:
        lt_cap = TRIGGERS[stage]["lt_max_hrs"]
        gdf_lt = gdf_dist[gdf_dist["leadtime"] <= pd.Timedelta(hours=lt_cap)]
        if gdf_lt.empty:
            rain = None
        else:
            rain = _rain_in_window(
                gefs_issuetime,
                gdf_lt["valid_time"].min().date(),
                (gdf_lt["valid_time"].max() + pd.Timedelta(days=1)).date(),
            )
        exp_total = exp_by_stage[stage] + exp_obsv
        rain_trigger = bool(
            rain is not None and rain >= TRIGGERS[stage]["rain_mm"]
        )
        exp_trigger = exp_total > 0
        row[f"rain_{lt_cap}h"] = rain
        row[f"exp_fcast_{lt_cap}h"] = exp_by_stage[stage]
        row[f"exp_total_{lt_cap}h"] = exp_total
        row[f"rain_trigger_{stage}"] = rain_trigger
        row[f"exp_trigger_{stage}"] = exp_trigger
        row[f"{stage}_trigger"] = (
            rain_trigger or exp_trigger
        ) and not past_cutoff
    return row


def load_gefs_with_issue_times() -> pd.DataFrame:
    df_gefs_all = chirps_gefs.load_recent_chirps_gefs_mean_daily()
    df_gefs_all["issue_time_approx"] = df_gefs_all[
        "issue_date"
    ] + pd.Timedelta(hours=8, minutes=50)
    return df_gefs_all


def update_fcast_monitoring(clobber: bool = False):
    adm0 = codab.load_codab_from_blob().to_crs(3857)
    logger.info("Loading recent CHIRPS-GEFS data for Haiti.")
    df_gefs_all = load_gefs_with_issue_times()
    logger.info("Loading existing monitoring points.")
    df_existing_monitoring = load_existing_monitoring_points("fcast")
    logger.info("Loading NHC advisories from the storms DB.")
    df_issuances = storms_db.fetch_recent_issuances(MONITORING_START)

    names = {}
    dicts = []
    for _, iss in df_issuances.iterrows():
        atcf_id = iss["atcf_id"]
        issue_time = iss["issued_time"]  # naive UTC
        monitor_id = _create_monitor_id(atcf_id, "fcast", issue_time)
        if _should_skip_existing(monitor_id, df_existing_monitoring, clobber):
            continue
        if atcf_id not in names:
            names[atcf_id] = storms_db.fetch_storm_name(atcf_id)
        row = process_fcast_advisory(
            atcf_id, issue_time, df_gefs_all, adm0, name=names[atcf_id]
        )
        if row is not None:
            dicts.append(row)

    df_new_monitoring = pd.DataFrame(dicts)

    if df_new_monitoring.empty:
        logger.info("No new forecast data found.")
    else:
        logger.info(f"Found {len(df_new_monitoring)} new forecast points.")
    if clobber:
        df_monitoring_combined = df_new_monitoring
    else:
        df_monitoring_combined = pd.concat(
            [df_existing_monitoring, df_new_monitoring]
        )

    if df_monitoring_combined.empty:
        return
    df_monitoring_combined = df_monitoring_combined.sort_values(
        ["issue_time", "atcf_id"]
    )
    blob_name = (
        f"{blob.PROJECT_PREFIX}/monitoring/hti_fcast_monitoring_v2.parquet"
    )
    blob.upload_parquet_to_blob(blob_name, df_monitoring_combined, index=False)


def update_obsv_monitoring(clobber: bool = False):
    adm0 = codab.load_codab_from_blob().to_crs(3857)
    logger.info("Loading recent IMERG data for Haiti.")
    obsv_rain = imerg.load_imerg_from_postgres(recent=True)
    obsv_rain["roll2_sum"] = (
        obsv_rain["mean"].rolling(window=2, center=True, min_periods=1).sum()
    )
    # IMERG for day D lands ~15:00 UTC on D+1
    obsv_rain["issue_time"] = obsv_rain["date"] + pd.Timedelta(
        hours=15, days=1
    )
    df_existing_monitoring = load_existing_monitoring_points("obsv")

    logger.info("Loading observed tracks from the storms DB.")
    df_issuances = storms_db.fetch_recent_issuances(MONITORING_START)
    atcf_ids = df_issuances["atcf_id"].unique()

    dicts = []
    for atcf_id in atcf_ids:
        gdf_track = storms_db.fetch_obsv_track(atcf_id)
        if gdf_track.empty:
            continue
        gdf = _interp_track(gdf_track, "valid_time", adm0)
        name = storms_db.fetch_storm_name(atcf_id)

        for issue_time in obsv_rain["issue_time"]:
            monitor_id = _create_monitor_id(atcf_id, "obsv", issue_time)
            if _should_skip_existing(
                monitor_id, df_existing_monitoring, clobber
            ):
                continue
            rain_recent = obsv_rain[obsv_rain["issue_time"] <= issue_time]
            gdf_recent = gdf[gdf["valid_time"] <= issue_time]
            if gdf_recent.empty:
                logger.debug(
                    f"Skipping {monitor_id} as storm is not active yet."
                )
                continue
            if rain_recent.empty:
                continue
            if rain_recent["date"].max().date() - gdf_recent[
                "valid_time"
            ].max().date() > pd.Timedelta(days=1):
                logger.debug(
                    f"Skipping {monitor_id} as storm is no longer active."
                )
                continue

            # rainfall attribution window: dates the storm was within
            # D_THRESH of Haiti (same attribution as calibration)
            gdf_dist = gdf_recent[gdf_recent["distance"] < D_THRESH]
            if gdf_dist.empty:
                obsv_rain_val = None
                rainfall_relevant = False
            else:
                start_day = pd.Timestamp(gdf_dist["valid_time"].min().date())
                end_day_late = pd.Timestamp(
                    gdf_dist["valid_time"].max().date() + pd.Timedelta(days=1)
                )
                # rainfall no longer relevant once past the date the
                # storm left the zone
                rainfall_relevant = rain_recent["date"].max() <= end_day_late
                obsv_rain_f = rain_recent[
                    (rain_recent["date"] >= start_day)
                    & (rain_recent["date"] <= end_day_late)
                ]
                obsv_rain_val = obsv_rain_f["roll2_sum"].max()

            obsv_exp = exposure.obsv_exposure_at(atcf_id, issue_time)

            rain_trigger = bool(
                obsv_rain_val is not None
                and obsv_rain_val >= TRIGGERS["obsv"]["rain_mm"]
            )
            exp_trigger = obsv_exp > 0
            dicts.append(
                {
                    "monitor_id": monitor_id,
                    "atcf_id": atcf_id,
                    "name": name,
                    "issue_time": issue_time,
                    "min_dist": gdf_recent["distance"].min(),
                    "obsv_rain": obsv_rain_val,
                    "obsv_exp": obsv_exp,
                    "rainfall_relevant": rainfall_relevant,
                    "rain_trigger_obsv": rain_trigger,
                    "exp_trigger_obsv": exp_trigger,
                    "obsv_trigger": rain_trigger or exp_trigger,
                }
            )

    df_new_monitoring = pd.DataFrame(dicts)
    if df_new_monitoring.empty:
        logger.info("No new observational data found.")
    else:
        logger.info(
            f"Found {len(df_new_monitoring)} new observational points."
        )
    if clobber:
        df_monitoring_combined = df_new_monitoring
    else:
        df_monitoring_combined = pd.concat(
            [df_existing_monitoring, df_new_monitoring]
        )
    if df_monitoring_combined.empty:
        return
    blob_name = (
        f"{blob.PROJECT_PREFIX}/monitoring/hti_obsv_monitoring_v2.parquet"
    )
    blob.upload_parquet_to_blob(blob_name, df_monitoring_combined, index=False)
