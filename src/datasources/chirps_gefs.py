import datetime
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


def download_recent_chirps_gefs():
    adm0 = codab.load_codab_from_blob(admin_level=0)
    total_bounds = adm0.total_bounds

    issue_date_range = pd.date_range(
        start="2024-01-01",
        end=datetime.date.today() + pd.DateOffset(days=1),
        freq="D",
    )

    existing_files = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/"
        f"{CHIRPS_GEFS_BLOB_DIR}/"
        f"chirps-gefs-hti_issued-2024"
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


def download_all_chirps_gefs():
    """Download all CHIRPS GEFS
    Takes around 40 hours
    """
    adm0 = codab.load_codab_from_blob(admin_level=0)
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
    verbose: bool = False,
):
    """Download CHIRPS GEFS data for a specific issue and valid date."""
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
    output_path = (
        f"{blob.PROJECT_PREFIX}/{CHIRPS_GEFS_BLOB_DIR}/{output_filename}"
    )
    if output_path in existing_files and not clobber:
        if verbose:
            print(
                f"File for issue date {issue_date} "
                f"and valid date {valid_date} already exists"
            )
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
                    blob.upload_blob_data(output_path, f)
    except Exception as e:
        if verbose:
            print(
                f"Failed to process the file for issue date "
                f"{issue_date} and valid date {valid_date}: {str(e)}"
            )
    return


def load_chirps_gefs_raster(
    issue_date: pd.Timestamp, valid_date: pd.Timestamp
):
    """Load CHIRPS GEFS raster data for a specific issue and valid date."""
    filename = (
        f"chirps-gefs-hti_"
        f"issued-{issue_date.date()}_valid-{valid_date.date()}.tif"
    )
    data = blob.load_blob_data(
        f"{blob.PROJECT_PREFIX}/{CHIRPS_GEFS_BLOB_DIR}/{filename}"
    )
    blob_data = BytesIO(data)
    da = rxr.open_rasterio(blob_data)
    da = da.squeeze(drop=True)
    return da


def process_chirps_gefs(verbose: bool = False):
    """Calculate spatial mean from all historical CHIRPS-GEFS forecasts
    for Haiti.
    """
    adm0 = codab.load_codab_from_blob(admin_level=0)
    start_date = "2000-01-01"
    end_date = "2023-12-31"

    issue_date_range = pd.date_range(start=start_date, end=end_date, freq="D")

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


def process_recent_chirps_gefs(verbose: bool = False):
    """Process only 2024 CHIRPS-GEFS forecasts for Haiti."""
    try:
        existing_df = load_recent_chirps_gefs_mean_daily()
    except ResourceNotFoundError:
        existing_df = pd.DataFrame(
            columns=["issue_date", "valid_date", "mean"]
        )
    adm0 = codab.load_codab_from_blob(admin_level=0)
    issue_date_range = pd.date_range(
        start="2024-01-01",
        end=datetime.date.today() + pd.DateOffset(days=1),
        freq="D",
    )
    dfs = []
    for issue_date in tqdm(issue_date_range):
        if issue_date in existing_df["issue_date"].unique():
            if verbose:
                print(f"Skipping {issue_date}, already processed")
            continue
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

    updated_df = pd.concat(dfs + [existing_df], ignore_index=True)
    blob_name = (
        f"{blob.PROJECT_PREFIX}/processed/chirps/gefs/hti/"
        f"hti_chirps_gefs_mean_daily_2024.parquet"
    )
    blob.upload_parquet_to_blob(blob_name, updated_df)


def load_recent_chirps_gefs_mean_daily():
    return blob.load_parquet_from_blob(
        f"{blob.PROJECT_PREFIX}/processed/chirps/gefs/hti/"
        "hti_chirps_gefs_mean_daily_2024.parquet"
    )


def load_chirps_gefs_mean_daily():
    data = blob.load_blob_data(
        f"{blob.PROJECT_PREFIX}/processed/chirps/gefs/hti/"
        "hti_chirps_gefs_mean_daily_2000_2023.parquet"
    )
    return pd.read_parquet(BytesIO(data))
