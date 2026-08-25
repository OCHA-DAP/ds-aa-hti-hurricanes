"""Run the DGPC rainfall analysis over every storm in the Haiti set.

Pulls IMERG half-hourly precipitation for each storm's attribution window
and reduces it to the maximum rolling accumulation over each DGPC window
under all three spatial aggregations.

Requires working Earthdata credentials (IMERG_USERNAME / IMERG_PASSWORD).

Usage: uv run python pipelines/run_dgpc_rain.py [--storm AL142016]
"""

import argparse
import tempfile
from pathlib import Path

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


def _cache_window(da, atcf_id):
    """Persist one storm's half-hourly window to blob as netCDF.

    ``DataArray.to_netcdf()`` with no path only works through the scipy
    (netCDF-3) backend and needs a named variable, neither of which holds
    here — so write a real file with h5netcdf and upload the bytes.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / f"{atcf_id}.nc"
        da.rename("precipitation").to_netcdf(path, engine="h5netcdf")
        blob.upload_blob_data(
            f"{OUT_PREFIX}/imerg_hh/{atcf_id}.nc", path.read_bytes()
        )


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
        # re-downloading ~250 granules per storm.
        try:
            _cache_window(da, s.atcf_id)
        except Exception as exc:  # noqa: BLE001
            # Non-fatal: the stats are already computed. Log the reason
            # rather than a bare "could not cache" so it stays diagnosable.
            logger.warning(
                "could not cache IMERG window for %s: %s", s.atcf_id, exc
            )

    df = pd.DataFrame(records)
    if df.empty:
        # Never clobber a good result with an empty one: a failed run must
        # leave the previous output (and the published page) untouched.
        raise RuntimeError(
            f"no storms produced rainfall stats ({len(storms)} attempted); "
            "leaving rain_stats.parquet as it was"
        )

    if only:
        # A partial run merges into the stored table rather than replacing
        # it, so smoke-testing one storm cannot wipe the other 41.
        try:
            prev = blob.load_parquet_from_blob(
                f"{OUT_PREFIX}/rain_stats.parquet"
            )
            keep = prev[~prev.atcf_id.isin(df.atcf_id)]
            df = pd.concat([keep, df], ignore_index=True)
            logger.info("merged with %d existing rows", len(keep))
        except Exception:
            logger.info("no existing rain_stats.parquet to merge with")

    df = df.sort_values("season").reset_index(drop=True)
    blob.upload_parquet_to_blob(f"{OUT_PREFIX}/rain_stats.parquet", df)
    logger.info("wrote %d rows to %s/rain_stats.parquet", len(df), OUT_PREFIX)
    return df


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--storm", nargs="*", default=None)
    args = ap.parse_args()
    main(only=args.storm)
