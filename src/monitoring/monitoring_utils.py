from typing import Literal

import geopandas as gpd
import pandas as pd

from src.constants import D_THRESH, LT_CUTOFF_HRS, THRESHS
from src.datasources import chirps_gefs, codab, imerg, nhc
from src.utils import blob


def load_existing_monitoring_points(fcast_obsv: Literal["fcast", "obsv"]):
    blob_name = (
        f"{blob.PROJECT_PREFIX}/monitoring/hti_{fcast_obsv}_monitoring.parquet"
    )
    return blob.load_parquet_from_blob(blob_name)


def update_obsv_monitoring(clobber: bool = False, verbose: bool = False):
    adm0 = codab.load_codab_from_blob().to_crs(3857)
    obsv_tracks = nhc.load_recent_glb_obsv()
    obsv_tracks = obsv_tracks[obsv_tracks["basin"] == "al"]
    obsv_tracks = obsv_tracks.rename(columns={"id": "atcf_id"})
    obsv_tracks = obsv_tracks.sort_values("lastUpdate")

    obsv_rain = imerg.load_imerg_mean(version=7, recent=True)
    obsv_rain["roll2_sum"] = (
        obsv_rain["mean"].rolling(window=2, center=True, min_periods=1).sum()
    )
    obsv_rain["issue_time"] = obsv_rain["date"].apply(
        lambda x: x.tz_localize("UTC")
    ) + pd.Timedelta(hours=15, days=1)
    df_existing_monitoring = load_existing_monitoring_points("obsv")
    cols = ["latitude", "longitude", "intensity", "pressure"]

    dicts = []
    for atcf_id, group in obsv_tracks.groupby("atcf_id"):
        df_interp = (
            group.set_index("lastUpdate")[cols]
            .resample("30min")
            .interpolate()
            .reset_index()
        )
        gdf = gpd.GeoDataFrame(
            data=df_interp,
            geometry=gpd.points_from_xy(
                df_interp["longitude"], df_interp["latitude"]
            ),
            crs=4326,
        )
        gdf["hti_distance"] = (
            gdf.to_crs(3857).geometry.distance(adm0.iloc[0].geometry) / 1000
        )
        for issue_time in obsv_rain["issue_time"]:
            monitor_id = (
                f"{atcf_id}_obsv_{issue_time.isoformat().split('+')[0]}"
            )
            if (
                monitor_id in df_existing_monitoring["monitor_id"].unique()
                and not clobber
            ):
                if verbose:
                    print(f"already monitored for {monitor_id}")
                continue
            rain_recent = obsv_rain[obsv_rain["issue_time"] <= issue_time]
            gdf_recent = gdf[gdf["lastUpdate"] <= issue_time]
            if gdf_recent.empty:
                # skip as storm is not active yet
                continue
            if rain_recent["date"].max().date() - gdf_recent[
                "lastUpdate"
            ].max().date() > pd.Timedelta(days=1):
                # skip as storm is no longer active
                continue

            name = group[group["lastUpdate"] <= issue_time].iloc[-1]["name"]

            # closest pass
            landfall_row = gdf_recent.loc[gdf_recent["hti_distance"].idxmin()]
            closest_s = landfall_row["intensity"]
            landfall_start_day = landfall_row["lastUpdate"].date()
            landfall_end_day_late = landfall_start_day + pd.Timedelta(days=1)
            obsv_rain_landfall = rain_recent[
                (rain_recent["date"].dt.date >= landfall_start_day)
                & (rain_recent["date"].dt.date <= landfall_end_day_late)
            ]
            closest_p = obsv_rain_landfall["roll2_sum"].max()

            # obsv trigger
            gdf_dist = gdf_recent[gdf_recent["hti_distance"] < D_THRESH]
            max_s = gdf_dist["intensity"].max()
            start_day = pd.Timestamp(gdf_dist["lastUpdate"].min().date())
            end_day_late = pd.Timestamp(
                gdf_dist["lastUpdate"].max().date() + pd.Timedelta(days=1)
            )
            # rainfall is no longer relevant if past the date the storm left
            # the trigger zone
            rainfall_relevant = rain_recent["date"].max() <= end_day_late
            obsv_rain_f = rain_recent[
                (rain_recent["date"] >= start_day)
                & (rain_recent["date"] <= end_day_late)
            ]
            max_p = obsv_rain_f["roll2_sum"].max()
            obsv_trigger = (max_p > THRESHS["obsv"]["p"]) & (
                max_s > THRESHS["obsv"]["s"]
            )
            dicts.append(
                {
                    "monitor_id": monitor_id,
                    "atcf_id": atcf_id,
                    "name": name,
                    "issue_time": issue_time,
                    "min_dist": gdf_recent["hti_distance"].min(),
                    "closest_s": closest_s,
                    "closest_p": closest_p,
                    "obsv_s": max_s,
                    "obsv_p": max_p,
                    "rainfall_relevant": rainfall_relevant,
                    "obsv_trigger": obsv_trigger,
                }
            )

    df_new_monitoring = pd.DataFrame(dicts)
    if clobber:
        df_monitoring_combined = df_new_monitoring
    else:
        df_monitoring_combined = pd.concat(
            [df_existing_monitoring, df_new_monitoring]
        )
    blob_name = f"{blob.PROJECT_PREFIX}/monitoring/hti_obsv_monitoring.parquet"
    blob.upload_parquet_to_blob(blob_name, df_monitoring_combined, index=False)


def update_fcast_monitoring(clobber: bool = False, verbose: bool = False):
    adm0 = codab.load_codab_from_blob().to_crs(3857)
    df_gefs_all = chirps_gefs.load_recent_chirps_gefs_mean_daily()
    df_gefs_all["issue_time_approx"] = (
        df_gefs_all["issue_date"] + pd.Timedelta(hours=8, minutes=50)
    ).apply(lambda x: x.tz_localize("UTC"))
    df_existing_monitoring = load_existing_monitoring_points(
        fcast_obsv="fcast"
    )
    df_tracks = nhc.load_recent_glb_forecasts()
    df_tracks = df_tracks[df_tracks["basin"] == "al"]

    dicts = []
    for issue_time, issue_group in df_tracks.groupby("issuance"):
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
        for atcf_id, group in issue_group.groupby("id"):
            monitor_id = (
                f"{atcf_id}_fcast_{issue_time.isoformat().split('+')[0]}"
            )
            if (
                monitor_id in df_existing_monitoring["monitor_id"].unique()
                and not clobber
            ):
                if verbose:
                    print(f"already monitored for {monitor_id}")
                continue
            else:
                print(f"monitoring for {monitor_id}")

            cols = ["latitude", "longitude", "maxwind"]
            df_interp = (
                group.set_index("validTime")[cols]
                .resample("30min")
                .interpolate()
                .reset_index()
            )
            gdf = gpd.GeoDataFrame(
                df_interp,
                geometry=gpd.points_from_xy(
                    df_interp.longitude, df_interp.latitude
                ),
                crs="EPSG:4326",
            ).to_crs(3857)
            gdf["distance"] = (
                gdf.geometry.distance(adm0.iloc[0].geometry) / 1000
            )
            gdf["leadtime"] = gdf["validTime"] - issue_time

            # closest pass
            landfall_row = gdf.loc[gdf["distance"].idxmin()]
            time_to_landfall = landfall_row["leadtime"]
            landfall_start_date = landfall_row["validTime"].date()
            landfall_end_date = landfall_start_date + pd.Timedelta(days=1)
            gefs_landfall = gefs_issuetime[
                (gefs_issuetime["valid_date"].dt.date >= landfall_start_date)
                & (gefs_issuetime["valid_date"].dt.date <= landfall_end_date)
            ]
            landfall_p = gefs_landfall["roll2_sum"].max()
            landfall_s = landfall_row["maxwind"]

            gdf_dist = gdf[gdf["distance"] < D_THRESH]

            # action
            gdf_action = gdf_dist[
                gdf_dist["leadtime"]
                <= pd.Timedelta(days=THRESHS["action"]["lt_days"])
            ]
            action_start_date = gdf_action["validTime"].min().date()
            action_end_date = gdf_action[
                "validTime"
            ].max().date() + pd.Timedelta(days=1)
            gefs_action = gefs_issuetime[
                (gefs_issuetime["valid_date"].dt.date >= action_start_date)
                & (gefs_issuetime["valid_date"].dt.date <= action_end_date)
            ]
            action_s = gdf_action["maxwind"].max()
            action_p = gefs_action["roll2_sum"].max()
            action_trigger = (action_s >= THRESHS["action"]["s"]) & (
                action_p >= THRESHS["action"]["p"]
            )

            # readiness
            gdf_readiness = gdf_dist[
                gdf_dist["leadtime"]
                <= pd.Timedelta(days=THRESHS["readiness"]["lt_days"])
            ]
            readiness_start_date = gdf_readiness["validTime"].min().date()
            readiness_end_date = gdf_readiness[
                "validTime"
            ].max().date() + pd.Timedelta(days=1)
            gefs_readiness = gefs_issuetime[
                (gefs_issuetime["valid_date"].dt.date >= readiness_start_date)
                & (gefs_issuetime["valid_date"].dt.date <= readiness_end_date)
            ]
            readiness_s = gdf_readiness["maxwind"].max()
            readiness_p = gefs_readiness["roll2_sum"].max()
            readiness_trigger = (readiness_s >= THRESHS["readiness"]["s"]) & (
                readiness_p >= THRESHS["readiness"]["p"]
            )

            dicts.append(
                {
                    "monitor_id": monitor_id,
                    "atcf_id": atcf_id,
                    "name": group["name"].iloc[0],
                    "issue_time": issue_time,
                    "time_to_closest": time_to_landfall,
                    "closest_s": landfall_s,
                    "closest_p": landfall_p,
                    "past_cutoff": time_to_landfall
                    < pd.Timedelta(hours=LT_CUTOFF_HRS),
                    "min_dist": gdf["distance"].min(),
                    "action_s": action_s,
                    "action_p": action_p,
                    "action_trigger": action_trigger,
                    "readiness_s": readiness_s,
                    "readiness_p": readiness_p,
                    "readiness_trigger": readiness_trigger,
                }
            )

    df_new_monitoring = pd.DataFrame(dicts)

    if clobber:
        df_monitoring_combined = df_new_monitoring
    else:
        df_monitoring_combined = pd.concat(
            [df_existing_monitoring, df_new_monitoring]
        )

    df_monitoring_combined = df_monitoring_combined.sort_values(
        ["issue_time", "atcf_id"]
    )
    blob_name = (
        f"{blob.PROJECT_PREFIX}/monitoring/hti_fcast_monitoring.parquet"
    )
    blob.upload_parquet_to_blob(blob_name, df_monitoring_combined, index=False)
