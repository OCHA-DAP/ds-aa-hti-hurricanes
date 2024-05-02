import tempfile
from io import BytesIO

import pandas as pd
import rioxarray as rxr
import xarray as xr
from azure.core.exceptions import ResourceNotFoundError
from tqdm import tqdm

from src.datasources import codab
from src.utils import blob

CHIRPS_GEFS_URL = (
    "https://data.chc.ucsb.edu/products/EWX/data/forecasts/"
    "CHIRPS-GEFS_precip_v12/daily_16day/"
    "{iss_year}/{iss_month:02d}/{iss_day:02d}/"
    "data.{valid_year}.{valid_month:02d}{valid_day:02d}.tif"
)
CHIRPS_GEFS_BLOB_DIR = "raw/chirps/gefs/hti"


def download_all_chirps_gefs():
    """Download all CHIRPS GEFS
    Takes around 40 hours
    """
    adm0 = codab.load_codab(admin_level=0)
    total_bounds = adm0.total_bounds
    start_date = "2000-01-01"
    end_date = "2023-12-31"

    issue_date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    existing_files = blob.list_container_blobs(
        name_starts_with=CHIRPS_GEFS_BLOB_DIR
    )
    for issue_date in tqdm(issue_date_range):
        for leadtime in range(16):
            valid_date = issue_date + pd.Timedelta(days=leadtime)
            download_chirps_gefs(
                issue_date,
                valid_date,
                total_bounds,
                existing_files,
                clobber=False,
            )


def download_chirps_gefs(
    issue_date: pd.Timestamp,
    valid_date: pd.Timestamp,
    total_bounds,
    existing_files,
    clobber: bool = False,
):
    url = CHIRPS_GEFS_URL.format(
        iss_year=issue_date.year,
        iss_month=issue_date.month,
        iss_day=issue_date.day,
        valid_year=valid_date.year,
        valid_month=valid_date.month,
        valid_day=valid_date.day,
    )
    output_filename = (
        f"chirps-gefs-hti_issued-"
        f"{issue_date.date()}_valid-{valid_date.date()}.tif"
    )
    if (
        f"{CHIRPS_GEFS_BLOB_DIR}/{output_filename}" in existing_files
        and not clobber
    ):
        # print(
        #     f"File for issue date {issue_date} "
        #     f"and valid date {valid_date} already exists"
        # )
        return
    try:
        with rxr.open_rasterio(url) as da:
            da_aoi = da.rio.clip_box(*total_bounds)
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".tif"
            ) as tmpfile:
                temp_filename = tmpfile.name
                da_aoi.rio.to_raster(temp_filename, driver="COG")

                with open(temp_filename, "rb") as f:
                    blob.upload_blob_data(
                        f"{CHIRPS_GEFS_BLOB_DIR}/{output_filename}", f
                    )
    except Exception as e:
        print(
            f"Failed to process the file for issue date "
            f"{issue_date} and valid date {valid_date}: {str(e)}"
        )
    return


def load_chirps_gefs_raster(
    issue_date: pd.Timestamp, valid_date: pd.Timestamp
):
    filename = (
        f"chirps-gefs-hti_"
        f"issued-{issue_date.date()}_valid-{valid_date.date()}.tif"
    )
    data = blob.load_blob_data(f"{CHIRPS_GEFS_BLOB_DIR}/{filename}")
    blob_data = BytesIO(data)
    da = rxr.open_rasterio(blob_data)
    da = da.squeeze(drop=True)
    return da


def process_chirps_gefs():
    adm0 = codab.load_codab(admin_level=0)
    start_date = "2000-01-01"
    end_date = "2023-12-31"

    issue_date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    verbose = False

    dfs = []
    for issue_date in tqdm(issue_date_range):
        das_i = []
        for leadtime in range(16):
            valid_date = issue_date + pd.Timedelta(days=leadtime)
            try:
                da_in = load_chirps_gefs_raster(issue_date, valid_date)
                da_in["valid_date"] = valid_date
                das_i.append(da_in)
            except ResourceNotFoundError as e:
                if verbose:
                    print(f"{e} for {issue_date} {valid_date}")

        if das_i:
            da_i = xr.concat(das_i, dim="valid_date")
            da_i_clip = da_i.rio.clip(adm0.geometry, all_touched=True)
            df_in = (
                da_i_clip.mean(dim=["x", "y"])
                .to_dataframe(name="mean")["mean"]
                .reset_index()
            )
            df_in["issue_date"] = issue_date
            dfs.append(df_in)
        else:
            if verbose:
                print(f"no files for issue_date {issue_date}")

    df = pd.concat(dfs, ignore_index=True)
    data = df.to_parquet()
    blob_proc_dir = "processed/chirps/gefs/hti/"
    blob_name = "hti_chirps_gefs_mean_daily_2000_2023.parquet"
    blob.upload_blob_data(blob_proc_dir + blob_name, data)


def load_chirps_gefs_mean_daily():
    data = blob.load_blob_data(
        "processed/chirps/gefs/hti/"
        "hti_chirps_gefs_mean_daily_2000_2023.parquet"
    )
    return pd.read_parquet(BytesIO(data))
