import datetime
import sys
import tempfile
import time
from io import BytesIO

import pandas as pd
import rasterio
import rioxarray as rxr
import xarray as xr
from azure.core.exceptions import ResourceNotFoundError
from tqdm import tqdm

from src.datasources import codab
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

# CHIRPS2-GEFS (v12) was discontinued 2026-07-01; issue dates from then
# on come from the CHIRPS3-calibrated datastream (c3g).
CHIRPS_GEFS_URL = (
    "https://data.chc.ucsb.edu/products/EWX/data/forecasts/"
    "CHIRPS-GEFS_precip_v12/daily_16day/"
    "{iss_year}/{iss_month:02d}/{iss_day:02d}/"
    "data.{valid_year}.{valid_month:02d}{valid_day:02d}.tif"
)
CHIRPS3_GEFS_URL = (
    "https://data.chc.ucsb.edu/products/CHIRPS-GEFS/v3/daily/global/"
    "{iss_year}/{iss_month:02d}/{iss_day:02d}/"
    "c3g_{valid_year}.{valid_month:02d}.{valid_day:02d}.tif"
)
CHIRPS3_START = pd.Timestamp("2026-07-01")
CHIRPS_GEFS_BLOB_DIR = "raw/chirps/gefs/hti"
N_LEADTIMES = 16
# Issue dates this recent are re-downloaded and re-processed until all
# N_LEADTIMES files are in: a run cut short (e.g. by the CHC server
# refusing requests) must not leave a partial issuance that the
# monitoring then reads as the latest forecast.
RECHECK_DAYS = 7
# Since late October 2026 data.chc.ucsb.edu refuses (HTTP 403) every
# request from an address after a short burst (first seen 2026-10-26:
# refused after 2 files). On each open GDAL by default lists the
# directory (403 on this server) and probes for 9 sidecar files (404s),
# so 10 of its 13 requests per file fail; turn both off so a download
# is only the 3 requests for the file itself.
CHC_GDAL_ENV = {
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif",
}
CHC_PAUSE_S = 1.0


def _issue_date(blob_name: str) -> pd.Timestamp:
    return pd.Timestamp(blob_name.split("issued-")[1].split("_valid-")[0])


def download_recent_chirps_gefs():
    adm0 = codab.load_codab_from_blob(admin_level=0)
    total_bounds = adm0.total_bounds

    current_year = datetime.date.today().year
    issue_date_range = pd.date_range(
        start=f"{current_year}-01-01",
        end=datetime.date.today() + pd.DateOffset(days=1),
        freq="D",
    )

    existing_files = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/"
        f"{CHIRPS_GEFS_BLOB_DIR}/"
        f"chirps-gefs-hti_issued-{current_year}"
    )

    n_files = pd.Series(
        [_issue_date(f) for f in existing_files], dtype="datetime64[ns]"
    ).value_counts()
    logger.info(
        f"Found {len(existing_files)} existing files for {current_year}"
    )
    recheck_from = pd.Timestamp(datetime.date.today()) - pd.Timedelta(
        days=RECHECK_DAYS
    )
    download_dates = [
        d
        for d in issue_date_range
        if n_files.get(d, 0) == 0
        or (d >= recheck_from and n_files.get(d, 0) < N_LEADTIMES)
    ]
    logger.info(
        f"Downloading {len(download_dates)} new issue dates for {current_year}: {[str(x.date()) for x in download_dates]}"  # noqa: E501
    )

    for issue_date in tqdm(
        download_dates,
        disable=not sys.stdout.isatty(),
    ):
        for leadtime in range(N_LEADTIMES):
            valid_date = issue_date + pd.Timedelta(days=leadtime)
            if not download_chirps_gefs(
                issue_date,
                valid_date,
                total_bounds,
                existing_files,
                clobber=False,
            ):
                # The rest of this issuance is not up yet, or the server
                # is refusing us: stop rather than add failed requests.
                break


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
) -> bool:
    """Download CHIRPS GEFS data for a specific issue and valid date.

    Returns False if the download failed, True otherwise (including when
    the file was already there).
    """
    url_template = (
        CHIRPS3_GEFS_URL if issue_date >= CHIRPS3_START else CHIRPS_GEFS_URL
    )
    url = url_template.format(
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
        return True
    ok = True
    try:
        with rasterio.Env(**CHC_GDAL_ENV), rxr.open_rasterio(url) as da:
            da_aoi = da.rio.clip_box(*total_bounds)
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".tif"
            ) as tmpfile:
                temp_filename = tmpfile.name
                da_aoi.rio.to_raster(temp_filename, driver="COG")
                with open(temp_filename, "rb") as f:
                    blob.upload_blob_data(output_path, f)
    except Exception as e:
        logger.warning(
            f"Failed to download or process CHIRPS GEFS data for "
            f"{issue_date.date()} valid {valid_date.date()}: {e}"
        )
        ok = False
    time.sleep(CHC_PAUSE_S)
    return ok


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
        logger.warning(
            "No existing data found for recent CHIRPS-GEFS mean daily."
        )
        existing_df = pd.DataFrame(
            columns=["issue_date", "valid_date", "mean"]
        )
    adm0 = codab.load_codab_from_blob(admin_level=0)
    issue_date_range = pd.date_range(
        start="2024-01-01",
        end=datetime.date.today() + pd.DateOffset(days=1),
        freq="D",
    )
    # A recent issuance processed from fewer than N_LEADTIMES files is
    # dropped and processed again, now that more files may be in.
    recheck_from = pd.Timestamp(datetime.date.today()) - pd.Timedelta(
        days=RECHECK_DAYS
    )
    n_rows = existing_df.groupby("issue_date")["valid_date"].nunique()
    partial = n_rows[
        (n_rows.index >= recheck_from) & (n_rows < N_LEADTIMES)
    ].index
    if len(partial):
        logger.info(
            "Reprocessing partial issuances: "
            f"{[str(pd.Timestamp(x).date()) for x in partial]}"
        )
        existing_df = existing_df[~existing_df["issue_date"].isin(partial)]
    unprocessed_issue_date_range = [
        d
        for d in issue_date_range
        if d not in existing_df["issue_date"].unique()
    ]
    logger.info(
        f"Processing {len(unprocessed_issue_date_range)} new issue dates "
        "for recent CHIRPS-GEFS: "
        f"{[str(x.date()) for x in unprocessed_issue_date_range]}"
    )
    dfs = []
    for issue_date in tqdm(
        unprocessed_issue_date_range, disable=not sys.stdout.isatty()
    ):
        if issue_date in existing_df["issue_date"].unique():
            if verbose:
                print(f"Skipping {issue_date}, already processed")
            continue
        das_i = []
        for leadtime in range(N_LEADTIMES):
            valid_date = issue_date + pd.Timedelta(days=leadtime)
            try:
                da_in = load_chirps_gefs_raster(issue_date, valid_date)
                da_in["valid_date"] = valid_date
                das_i.append(da_in)
            except ResourceNotFoundError as e:
                if verbose:
                    print(f"{e} for {issue_date} {valid_date}")

        if das_i and issue_date >= recheck_from and len(das_i) < N_LEADTIMES:
            logger.warning(
                f"Only {len(das_i)} of {N_LEADTIMES} files for recent "
                f"issue_date {issue_date.date()}; not processed until "
                "complete."
            )
        elif das_i:
            logger.info(
                f"Processing {len(das_i)} files for issue_date {issue_date}"
            )
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
            logger.warning(
                f"No files found for issue_date {issue_date.date()}, skipping."
            )

    updated_df = pd.concat(dfs + [existing_df], ignore_index=True)
    blob_name = (
        f"{blob.PROJECT_PREFIX}/processed/chirps/gefs/hti/"
        f"hti_chirps_gefs_mean_daily_since2024.parquet"
    )
    blob.upload_parquet_to_blob(blob_name, updated_df)


def load_recent_chirps_gefs_mean_daily():
    return blob.load_parquet_from_blob(
        f"{blob.PROJECT_PREFIX}/processed/chirps/gefs/hti/"
        "hti_chirps_gefs_mean_daily_since2024.parquet"
    )


def load_chirps_gefs_mean_daily():
    data = blob.load_blob_data(
        f"{blob.PROJECT_PREFIX}/processed/chirps/gefs/hti/"
        "hti_chirps_gefs_mean_daily_2000_2023.parquet"
    )
    return pd.read_parquet(BytesIO(data))
