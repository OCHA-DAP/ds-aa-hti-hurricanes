"""Run the DGPC rainfall analysis over every storm in the Haiti set.

Pulls IMERG half-hourly precipitation for each storm's attribution window
and reduces it to the maximum rolling accumulation over each DGPC window
under all three spatial aggregations.

Requires working Earthdata credentials (IMERG_USERNAME / IMERG_PASSWORD).

Usage: uv run python pipelines/run_dgpc_rain.py [--storm AL142016]
"""

import argparse

import pandas as pd
from tqdm import tqdm

from src.datasources import imerg_hh
from src.dgpc.rain_analysis import (
    evaluate_criteria,
    storm_rain_stats,
    storm_window,
)
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

OUT_PREFIX = f"{blob.PROJECT_PREFIX}/processed/dgpc"


def main(only=None):
    storms = blob.load_parquet_from_blob(f"{OUT_PREFIX}/storm_set.parquet")
    if only:
        storms = storms[storms.atcf_id.isin(only)]
    logger.info("%d storms to process", len(storms))

    records = []
    for _, s in tqdm(storms.iterrows(), total=len(storms), desc="storms"):
        start, end = storm_window(s.first_near, s.last_near)
        try:
            da = imerg_hh.fetch_window(start, end)
        except Exception:
            logger.exception("IMERG fetch failed for %s", s.atcf_id)
            continue

        rec = {
            "atcf_id": s.atcf_id,
            "name": s["name"],
            "season": s.season,
            "window_start": start,
            "window_end": end,
            "n_timesteps": int(da.sizes["time"]),
        }
        stats = storm_rain_stats(da)
        rec.update(stats)
        rec.update(evaluate_criteria(stats))
        records.append(rec)

        # Cache the raw window so the analysis can be re-run without
        # re-downloading ~300 granules per storm.
        try:
            blob.upload_blob_data(
                f"{OUT_PREFIX}/imerg_hh/{s.atcf_id}.nc",
                da.to_netcdf(),
            )
        except Exception:
            logger.warning("could not cache IMERG window for %s", s.atcf_id)

    df = pd.DataFrame(records)
    blob.upload_parquet_to_blob(f"{OUT_PREFIX}/rain_stats.parquet", df)
    logger.info("wrote %d rows to %s/rain_stats.parquet", len(df), OUT_PREFIX)
    return df


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--storm", nargs="*", default=None)
    args = ap.parse_args()
    main(only=args.storm)
