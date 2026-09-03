"""Every trigger pathway per storm, plus the cutoff-aware department table.

Reads the storm set, the per-issuance NHC wind fields, the CHIRPS-GEFS
department rasters (``run_dgpc_dept_forecast.py``), the storms DB
exposure tables and the calibration backtest, and writes under
``processed/dgpc/``:

- ``pathways.parquet``            - one row per storm, every indicator
- ``pathways_advisories.parquet`` - one row per NHC advisory
- ``dept_verdicts.parquet``       - one row per storm x department, on
                                    pre-cutoff advisories, gust reading

Usage: uv run python pipelines/run_dgpc_pathways.py
"""

from src.dgpc import pathways as pw
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    df, adv = pw.build()
    storms = blob.load_parquet_from_blob(f"{pw.PREFIX}/storm_set.parquet")
    fcast = blob.load_parquet_from_blob(
        f"{pw.PREFIX}/fcast_wind_by_issuance.parquet"
    )
    rain_long = blob.load_parquet_from_blob(
        f"{pw.PREFIX}/dept_rain_fcast.parquet"
    )
    depts = pw.dept_table(storms, adv, fcast, rain_long)

    blob.upload_parquet_to_blob(f"{pw.PREFIX}/pathways.parquet", df)
    blob.upload_parquet_to_blob(
        f"{pw.PREFIX}/pathways_advisories.parquet", adv
    )
    blob.upload_parquet_to_blob(f"{pw.PREFIX}/dept_verdicts.parquet", depts)
    logger.info(
        "wrote %d storms, %d advisories, %d storm-department rows",
        len(df),
        len(adv),
        len(depts),
    )
    return df, adv, depts


if __name__ == "__main__":
    main()
