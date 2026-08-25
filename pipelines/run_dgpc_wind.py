"""Run the DGPC wind analysis over every storm in the Haiti set.

Writes three parquet files to blob under
``ds-aa-hti-hurricanes/processed/dgpc/``:

- ``fcast_wind_by_issuance`` - one row per storm x NHC forecast issuance
- ``obsv_wind`` - one row per storm, from the IBTrACS best track
- ``storm_set`` - the storms considered, with closest approach

Also runs the Rmw sensitivity (x0.79 / x1.40, the observed IQR around the
climatological Rmw at >=108 kt) for the red wind level only.

Usage: uv run python pipelines/run_dgpc_wind.py
"""

import pandas as pd
from tqdm import tqdm

from src.dgpc.wind_analysis import (
    forecast_wind_by_issuance,
    observed_wind,
    storm_set,
)
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

OUT_PREFIX = f"{blob.PROJECT_PREFIX}/processed/dgpc"
RMW_SENSITIVITY = {"lo": 0.79, "hi": 1.40}


def main():
    storms = storm_set()
    logger.info("%d storms in the Haiti set", len(storms))
    blob.upload_parquet_to_blob(f"{OUT_PREFIX}/storm_set.parquet", storms)

    fcast, obsv = [], []
    for _, s in tqdm(storms.iterrows(), total=len(storms), desc="storms"):
        aid = s.atcf_id
        try:
            df = forecast_wind_by_issuance(aid)
            if not df.empty:
                fcast.append(df)
        except Exception:
            logger.exception("forecast wind failed for %s", aid)

        try:
            rec = observed_wind(aid, closest_time=s.closest_time)
            if rec:
                for tag, scale in RMW_SENSITIVITY.items():
                    alt = observed_wind(
                        aid, rmw_scale=scale,
                        closest_time=s.closest_time,
                    )
                    rec[f"sustained_land_max_kt_rmw_{tag}"] = alt.get(
                        "sustained_land_max_kt"
                    )
                    rec[f"sustained_land_pop_red_rmw_{tag}"] = alt.get(
                        "sustained_land_pop_red"
                    )
                obsv.append(rec)
        except Exception:
            logger.exception("observed wind failed for %s", aid)

    df_f = pd.concat(fcast, ignore_index=True) if fcast else pd.DataFrame()
    df_o = pd.DataFrame(obsv)
    logger.info("forecast rows %d, observed rows %d", len(df_f), len(df_o))

    blob.upload_parquet_to_blob(
        f"{OUT_PREFIX}/fcast_wind_by_issuance.parquet", df_f
    )
    blob.upload_parquet_to_blob(f"{OUT_PREFIX}/obsv_wind.parquet", df_o)
    logger.info("wrote results to %s", OUT_PREFIX)


if __name__ == "__main__":
    main()
