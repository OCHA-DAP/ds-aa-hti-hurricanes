import gzip
from datetime import datetime
from ftplib import FTP
from io import BytesIO

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from src.constants import D_THRESH
from src.datasources import chirps_gefs, codab, ibtracs
from src.utils import blob


def calculate_hist_fcast_monitors():
    sid_atcf = ibtracs.load_ibtracs_sid_atcf_names()
    sid_atcf.loc[:, "name"] = sid_atcf["name"].str.capitalize()
    sid_atcf.loc[:, "usa_atcf_id"] = sid_atcf["usa_atcf_id"].str.lower()
    sid_atcf = sid_atcf.rename(columns={"usa_atcf_id": "atcf_id"})

    rain = chirps_gefs.load_chirps_gefs_mean_daily()
    rain["roll2_sum_bw"] = (
        rain.groupby("issue_date")["mean"]
        .rolling(window=2, center=True, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )
    date_cols = ["issue_date", "valid_date"]
    for date_col in date_cols:
        rain[date_col] = rain[date_col].dt.date

    all_tracks = load_hti_distances()
    close_tracks = all_tracks[all_tracks["hti_distance_km"] < D_THRESH]
    close_atcf_ids = close_tracks["atcf_id"].unique()
    tracks = all_tracks[all_tracks["atcf_id"].isin(close_atcf_ids)].copy()
    tracks["lt"] = tracks["valid_time"] - tracks["issue_time"]

    lts = {
        "readiness": pd.Timedelta(days=5),
        "action": pd.Timedelta(days=3),
        "obsv": pd.Timedelta(days=0),
    }

    dicts = []
    for atcf_id, storm_group in tqdm(tracks.groupby("atcf_id")):
        for issue_time, issue_group in storm_group.groupby("issue_time"):
            rain_i = rain[rain["issue_date"] == issue_time.date()]
            for lt_name, lt in lts.items():
                dff_lt = issue_group[
                    issue_group["valid_time"] <= issue_time + lt
                ]
                dff_dist = dff_lt[dff_lt["hti_distance_km"] <= D_THRESH]
                start_date = dff_dist["valid_time"].min().date()
                end_date = dff_dist["valid_time"].max().date() + pd.Timedelta(
                    days=1
                )
                dicts.append(
                    {
                        "atcf_id": atcf_id,
                        "issue_time": issue_time,
                        "lt_name": lt_name,
                        "roll2_rain_dist": rain_i[
                            (rain_i["valid_date"] >= start_date)
                            & (rain_i["valid_date"] <= end_date)
                        ]["roll2_sum_bw"].max(),
                        "wind_dist": dff_dist["windspeed"].max(),
                        "dist_min": dff_lt["hti_distance_km"].min(),
                        "roll2_rain": rain_i[
                            rain_i["valid_date"] <= (issue_time + lt).date()
                        ]["roll2_sum_bw"].max(),
                        "wind": dff_lt["windspeed"].max(),
                    }
                )

    monitors = pd.DataFrame(dicts)
    monitors = monitors.merge(sid_atcf)
    save_blob = "ds-aa-hti-hurricanes/processed/monitors.parquet"
    blob.upload_blob_data(save_blob, monitors.to_parquet(), prod_dev="dev")


def load_hist_fcast_monitors():
    return pd.read_parquet(
        BytesIO(
            blob.load_blob_data(
                "ds-aa-hti-hurricanes/processed/monitors.parquet",
                prod_dev="dev",
            )
        )
    )


def download_historical_forecasts(
    clobber: bool = False,
    include_archive: bool = True,
    include_recent: bool = True,
):
    # cols from
    # https://www.nrlmry.navy.mil/atcf_web/docs/database/new/abdeck.txt
    # tech list from https://ftp.nhc.noaa.gov/atcf/docs/nhc_techlist.dat
    nhc_cols_str = (
        "BASIN, CY, YYYYMMDDHH, TECHNUM/MIN, TECH, TAU, LatN/S, "
        "LonE/W, VMAX, MSLP, TY, RAD, WINDCODE, RAD1, RAD2, RAD3, "
        "RAD4, POUTER, ROUTER, RMW, GUSTS, EYE, SUBREGION, MAXSEAS, "
        "INITIALS, DIR, SPEED, STORMNAME, DEPTH, SEAS, SEASCODE, "
        "SEAS1, SEAS2, SEAS3, SEAS4"
    )
    nhc_cols = nhc_cols_str.split(", ")
    nhc_cols.extend(
        [y + str(x) for x in range(1, 21) for y in ["USERDEFINE", "userdata"]]
    )

    ftp_server = "ftp.nhc.noaa.gov"
    ftp = FTP(ftp_server)
    ftp.login("", "")
    recent_directory = "/atcf/aid_public"
    archive_directory = "/atcf/archive"

    existing_files = blob.list_container_blobs(name_starts_with="raw/noaa/nhc")
    if include_archive:
        ftp.cwd(archive_directory)
        for year in tqdm(range(2000, 2023)):
            if ftp.pwd() != archive_directory:
                ftp.cwd("..")
            ftp.cwd(str(year))
            filenames = [
                x
                for x in ftp.nlst()
                if x.endswith(".dat.gz") and x.startswith("aal")
            ]
            for filename in filenames:
                out_blob = (
                    f"raw/noaa/nhc/historical_forecasts/{year}/"
                    f"{filename.removesuffix('.dat.gz')}.csv"
                )
                if out_blob in existing_files and not clobber:
                    continue
                with BytesIO() as buffer:
                    ftp.retrbinary("RETR " + filename, buffer.write)
                    buffer.seek(0)
                    with gzip.open(buffer, "rt") as file:
                        df = pd.read_csv(file, header=None, names=nhc_cols)
                out_data = df.to_csv(index=False)

                blob.upload_blob_data(out_blob, out_data)

            ftp.cwd("..")
        ftp.cwd("..")

    if include_recent:
        ftp.cwd(recent_directory)
        filenames = [
            x
            for x in ftp.nlst()
            if x.endswith(".dat.gz") and x.startswith("aal")
        ]
        for filename in filenames:
            out_blob = (
                f"raw/noaa/nhc/historical_forecasts/recent/"
                f"{filename.removesuffix('.dat.gz')}.csv"
            )
            if out_blob in existing_files and not clobber:
                continue
            with BytesIO() as buffer:
                ftp.retrbinary("RETR " + filename, buffer.write)
                buffer.seek(0)
                with gzip.open(buffer, "rt") as file:
                    df = pd.read_csv(file, header=None, names=nhc_cols)
            out_data = df.to_csv(index=False)

            blob.upload_blob_data(out_blob, out_data)

        ftp.cwd("..")


def process_historical_forecasts():
    blob_names = blob.list_container_blobs(
        name_starts_with="raw/noaa/nhc/historical_forecasts/"
    )
    blob_names = [x for x in blob_names if x.endswith(".csv")]

    def proc_latlon(latlon):
        c = latlon[-1]
        if c in ["N", "E"]:
            return float(latlon[:-1]) / 10
        elif c in ["S", "W"]:
            return -float(latlon[:-1]) / 10

    dfs = []
    for blob_name in tqdm(blob_names):
        df_in = pd.read_csv(BytesIO(blob.load_blob_data(blob_name)))
        atcf_id = blob_name.removesuffix(".csv")[-8:]

        cols = ["YYYYMMDDHH", "TAU", "LatN/S", "LonE/W", "MSLP", "VMAX"]
        dff = df_in[df_in["TECH"] == " OFCL"][cols]
        if dff.empty:
            continue

        dff["issue_time"] = dff["YYYYMMDDHH"].apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d%H")
        )
        dff["valid_time"] = dff.apply(
            lambda row: row["issue_time"] + pd.Timedelta(hours=row["TAU"]),
            axis=1,
        )

        dff["lat"] = dff["LatN/S"].apply(proc_latlon)
        dff["lon"] = dff["LonE/W"].apply(proc_latlon)
        dff = dff.rename(
            columns={
                "TAU": "leadtime",
                "MSLP": "pressure",
                "VMAX": "windspeed",
            }
        )
        cols = [
            "issue_time",
            "valid_time",
            "lat",
            "lon",
            "windspeed",
            "pressure",
        ]
        dff = dff[cols]
        dff = dff.loc[~dff.duplicated()]
        dff["atcf_id"] = atcf_id
        dfs.append(dff)

    df = pd.concat(dfs, ignore_index=True)
    save_blob = "processed/noaa/nhc/historical_forecasts/al_2000_2023.csv"
    blob.upload_blob_data(save_blob, df.to_csv(index=False))


def load_processed_historical_forecasts():
    return pd.read_csv(
        BytesIO(
            blob.load_blob_data(
                "processed/noaa/nhc/historical_forecasts/al_2000_2023.csv"
            )
        ),
        parse_dates=["issue_time", "valid_time"],
    )


def calculate_hti_distance():
    df = load_processed_historical_forecasts()
    adm0 = codab.load_codab(admin_level=0)
    adm0 = adm0.to_crs(3857)

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs="EPSG:4326",
    )
    gdf = gdf.to_crs(3857)

    gdf["hti_distance"] = gdf.geometry.distance(adm0.iloc[0].geometry)
    gdf["hti_distance_km"] = gdf["hti_distance"] / 1000

    save_blob = (
        "processed/noaa/nhc/historical_forecasts/"
        "hti_distances_2000_2023.parquet"
    )
    data = gdf.drop(columns=["hti_distance", "geometry"]).to_parquet()
    blob.upload_blob_data(save_blob, data)


def load_hti_distances():
    return pd.read_parquet(
        BytesIO(
            blob.load_blob_data(
                "processed/noaa/nhc/historical_forecasts/"
                "hti_distances_2000_2023.parquet"
            )
        )
    )
