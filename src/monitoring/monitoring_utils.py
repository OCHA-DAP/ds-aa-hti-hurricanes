from typing import Literal

import geopandas as gpd
import pandas as pd

from src.datasources import chirps_gefs, codab, nhc
from src.utils import blob

D_THRESH = 230
LT_CUTOFF_HRS = 36


def load_existing_monitoring_points(fcast_obsv: Literal["fcast", "obsv"]):
    blob_name = (
        f"{blob.PROJECT_PREFIX}/monitoring/hti_{fcast_obsv}_monitoring.parquet"
    )
    return blob.load_parquet_from_blob(blob_name)


def update_fcast_monitoring(clobber: bool = False):
    adm0 = codab.load_codab_from_blob().to_crs(3857)
    df_gefs_all = chirps_gefs.load_recent_chirps_gefs_mean_daily()
    df_gefs_all["issue_time_approx"] = (
        df_gefs_all["issue_date"] + pd.Timedelta(hours=8, minutes=50)
    ).apply(lambda x: x.tz_localize("UTC"))
    df_existing_monitoring = load_existing_monitoring_points(
        fcast_obsv="fcast"
    )
    df_existing = nhc.load_recent_glb_forecasts()
    df_existing = df_existing[df_existing["basin"] == "al"]

    threshs = {
        "readiness": {"p": 35, "s": 34, "lt_days": 5},
        "action": {"p": 42, "s": 64, "lt_days": 3},
    }

    dicts = []
    for issue_time, issue_group in df_existing.groupby("issuance"):
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
            landfall_row = gdf.loc[gdf["distance"].idxmin()]
            time_to_landfall = landfall_row["leadtime"]

            gdf_dist = gdf[gdf["distance"] < D_THRESH]

            # action
            gdf_action = gdf_dist[
                gdf_dist["leadtime"]
                <= pd.Timedelta(days=threshs["action"]["lt_days"])
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
            action_trigger = (action_s >= threshs["action"]["s"]) & (
                action_p >= threshs["action"]["p"]
            )

            # readiness
            gdf_readiness = gdf_dist[
                gdf_dist["leadtime"]
                <= pd.Timedelta(days=threshs["readiness"]["lt_days"])
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
            readiness_trigger = (readiness_s >= threshs["readiness"]["s"]) & (
                readiness_p >= threshs["readiness"]["p"]
            )

            dicts.append(
                {
                    "monitor_id": monitor_id,
                    "atcf_id": atcf_id,
                    "name": group["name"].iloc[0],
                    "issue_time": issue_time,
                    "time_to_closest": time_to_landfall,
                    "closest_s": landfall_row["maxwind"],
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
    blob.upload_parquet_to_blob(blob_name, df_monitoring_combined)
