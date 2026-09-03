"""DGPC orange alert by department, on forecasts, for every storm.

Reads the storm set and the per-issuance NHC wind fields written by
``run_dgpc_wind.py``, pulls the CHIRPS-GEFS daily rasters for each storm's
approach from blob, and writes under ``processed/dgpc/``:

- ``dept_rain_fcast.parquet``  - one row per storm x GEFS issue x valid
                                 day x department (mean, pixel max)
- ``dept_verdicts.parquet``    - one row per storm x department

Usage: uv run python pipelines/run_dgpc_dept_forecast.py
"""

import pandas as pd
from tqdm import tqdm

from src.datasources import codab
from src.dgpc.dept_forecast import (
    department_verdicts,
    storm_rain_forecast,
    wind_forecast_by_dept,
)
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

OUT_PREFIX = f"{blob.PROJECT_PREFIX}/processed/dgpc"


def main():
    storms = blob.load_parquet_from_blob(f"{OUT_PREFIX}/storm_set.parquet")
    fcast = blob.load_parquet_from_blob(
        f"{OUT_PREFIX}/fcast_wind_by_issuance.parquet"
    )
    dept_names = (
        codab.load_codab_from_blob(admin_level=1)
        .sort_values("ADM1_PCODE")["ADM1_FR"]
        .tolist()
    )

    frames, coverage = [], []
    for _, s in tqdm(storms.iterrows(), total=len(storms), desc="storms"):
        df, wanted, missing = storm_rain_forecast(s)
        coverage.append(
            {
                "atcf_id": s.atcf_id,
                "gefs_wanted": wanted,
                "gefs_missing": missing,
            }
        )
        if missing:
            logger.warning(
                "%s: %d of %d CHIRPS-GEFS rasters missing",
                s.atcf_id,
                missing,
                wanted,
            )
        if len(df):
            frames.append(df)
    rain_long = pd.concat(frames, ignore_index=True)
    blob.upload_parquet_to_blob(
        f"{OUT_PREFIX}/dept_rain_fcast.parquet", rain_long
    )

    wind_long = wind_forecast_by_dept(fcast)
    verdicts = department_verdicts(rain_long, wind_long, storms, dept_names)
    verdicts = verdicts.merge(pd.DataFrame(coverage), on="atcf_id")
    blob.upload_parquet_to_blob(
        f"{OUT_PREFIX}/dept_verdicts.parquet", verdicts
    )
    logger.info(
        "wrote %d rain rows and %d verdict rows to %s",
        len(rain_long),
        len(verdicts),
        OUT_PREFIX,
    )
    return verdicts


if __name__ == "__main__":
    main()
