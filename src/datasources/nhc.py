import gzip
from datetime import datetime
from ftplib import FTP
from io import BytesIO

import pandas as pd
from tqdm import tqdm

from src.utils import blob


def download_archive_forecasts(clobber: bool = False):
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
    archive_directory = "/atcf/archive"
    ftp.cwd(archive_directory)

    existing_files = blob.list_container_blobs(name_starts_with="raw/noaa/nhc")
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


def process_archive_forecasts():
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
    save_blob = "processed/noaa/nhc/historical_forecasts/al_2000_2022.csv"
    blob.upload_blob_data(save_blob, df.to_csv(index=False))
