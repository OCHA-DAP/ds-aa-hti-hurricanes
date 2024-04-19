import tempfile
from io import BytesIO

import pandas as pd
import rioxarray as rxr

from src.utils import blob

CHIRPS_GEFS_URL = (
    "https://data.chc.ucsb.edu/products/EWX/data/forecasts/"
    "CHIRPS-GEFS_precip_v12/daily_16day/"
    "{iss_year}/{iss_month:02d}/{iss_day:02d}/"
    "data.{valid_year}.{valid_month:02d}{valid_day:02d}.tif"
)
CHIRPS_GEFS_BLOB_DIR = "raw/chirps/gefs/hti"


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
    return blob_data
