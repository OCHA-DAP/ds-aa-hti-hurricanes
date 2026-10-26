import datetime
import sys

import pandas as pd

from src.datasources.chirps_gefs import (
    N_LEADTIMES,
    download_recent_chirps_gefs,
    load_recent_chirps_gefs_mean_daily,
    process_recent_chirps_gefs,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting the CHIRPS-GEFS update pipeline...")

    logger.info("Downloading recent CHIRPS-GEFS data...")
    download_recent_chirps_gefs()

    logger.info("Processing recent CHIRPS-GEFS data...")
    process_recent_chirps_gefs()

    # Failed downloads are only logged, so check the result: the run must
    # leave yesterday's issuance (or a later one) complete. Otherwise exit
    # non-zero so the workflow's failure step opens an issue, instead of
    # the monitoring quietly reading an old forecast.
    df = load_recent_chirps_gefs_mean_daily()
    n = df.groupby("issue_date")["valid_date"].nunique()
    complete = n[n >= N_LEADTIMES].index
    latest = pd.Timestamp(complete.max()) if len(complete) else None
    expected = pd.Timestamp(datetime.date.today()) - pd.Timedelta(days=1)
    if latest is None or latest < expected:
        logger.error(
            f"Latest complete CHIRPS-GEFS issuance is {latest}; expected "
            f"{expected.date()} or later."
        )
        sys.exit(1)
    logger.info(f"Latest complete CHIRPS-GEFS issuance: {latest.date()}")
