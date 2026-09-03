"""DGPC orange alert by department, on the framework's forecasts.

DGPC issues alerts per department. This module asks, for every storm in
the Haiti set and every department: would the forecasts available at the
time have met the orange criterion there?

- **Rain** - CHIRPS-GEFS daily forecasts (the framework's rain source),
  read two ways per department: the department *mean* and the wettest
  *pixel* in the department. The DGPC criterion is 100 mm/24 h; the daily
  forecast value stands in for the 24 h accumulation.
- **Wind** - the NHC forecast wind field already computed per issuance in
  ``wind_analysis`` (parametric field, sustained over land), reduced to
  the maximum reaching each department.

A department is in orange for a storm if *any* forecast issued during the
storm's approach met the criterion there. Where CHIRPS-GEFS has no data
for the approach (the 2020 archive gap), the rain reading is undetermined,
not "not met".
"""

from functools import lru_cache

import numpy as np
import pandas as pd
import xarray as xr
from azure.core.exceptions import ResourceNotFoundError
from rasterio.features import rasterize
from rasterio.transform import from_origin

from src.datasources import chirps_gefs, codab
from src.dgpc import constants as dc
from src.utils.logging import get_logger
from src.utils.raster import upsample_dataarray

logger = get_logger(__name__)

RAIN_READINGS = {
    "rain_mean": "Pluie — moyenne du département",
    "rain_pix": "Pluie — point le plus arrosé du département",
}
READINGS = {
    "wind": "Vent ≥ 100 km/h dans le département",
    **RAIN_READINGS,
    "any_mean": "Vent OU pluie (moyenne)",
    "any_pix": "Vent OU pluie (point)",
}


@lru_cache(maxsize=2)
def _dept_masks(lat_key, lon_key):
    """Department index on the upsampled CHIRPS-GEFS grid.

    Keyed on the grid coordinates (as tuples) so it is built once.
    """
    lats = np.asarray(lat_key)
    lons = np.asarray(lon_key)
    adm1 = codab.load_codab_from_blob(admin_level=1).sort_values("ADM1_PCODE")
    names = adm1["ADM1_FR"].tolist()
    res_y = float(lats[1] - lats[0])
    res_x = float(lons[1] - lons[0])
    transform = from_origin(
        lons.min() - res_x / 2, lats.max() + abs(res_y) / 2, res_x, abs(res_y)
    )
    dept = rasterize(
        ((g, i) for i, g in enumerate(adm1.geometry)),
        out_shape=(len(lats), len(lons)),
        transform=transform,
        fill=-1,
        dtype="int32",
    )
    if res_y > 0:  # rasterize assumes north-up
        dept = dept[::-1]
    return dept, names


def dept_stats(da: xr.DataArray) -> pd.DataFrame:
    """Department mean and max of one CHIRPS-GEFS daily raster (mm)."""
    up = upsample_dataarray(
        da, resolution=dc.GEFS_UPSAMPLE_RES, lat_dim="y", lon_dim="x"
    )
    dept, names = _dept_masks(
        tuple(np.round(up["y"].to_numpy(), 6)),
        tuple(np.round(up["x"].to_numpy(), 6)),
    )
    vals = up.to_numpy().astype(float)
    vals = np.where(vals < 0, np.nan, vals)  # nodata fill
    rows = []
    for i, nm in enumerate(names):
        sel = dept == i
        if not sel.any():
            continue
        v = vals[sel]
        if not np.isfinite(v).any():
            continue
        rows.append(
            {
                "dept": nm,
                "mean_mm": float(np.nanmean(v)),
                "pix_max_mm": float(np.nanmax(v)),
            }
        )
    return pd.DataFrame(rows)


def storm_rain_forecast(storm: pd.Series) -> tuple[pd.DataFrame, int, int]:
    """Every CHIRPS-GEFS (issue, valid) day attributable to one storm.

    Returns the long table plus (rasters wanted, rasters missing).
    """
    first = pd.Timestamp(storm.first_near).normalize()
    last = pd.Timestamp(storm.last_near).normalize()
    pad = pd.Timedelta(days=dc.RAIN_WINDOW_PAD_DAYS)
    valid_lo, valid_hi = first - pad, last + pad
    issue_dates = pd.date_range(
        first - pd.Timedelta(days=dc.FCAST_LEAD_DAYS), last, freq="D"
    )

    frames, wanted, missing = [], 0, 0
    for iss in issue_dates:
        for lt in range(16):
            vd = iss + pd.Timedelta(days=lt)
            if vd < valid_lo or vd > valid_hi:
                continue
            wanted += 1
            try:
                da = chirps_gefs.load_chirps_gefs_raster(iss, vd)
            except ResourceNotFoundError:
                missing += 1
                continue
            df = dept_stats(da)
            df["atcf_id"] = storm.atcf_id
            df["issue_date"] = iss
            df["valid_date"] = vd
            df["leadtime_days"] = lt
            frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return out, wanted, missing


def wind_forecast_by_dept(
    fcast: pd.DataFrame, variant: str = dc.PRIMARY_WIND_VARIANT
) -> pd.DataFrame:
    """Long table of per-department forecast wind, one row per issuance."""
    prefix = f"{variant}_dept_"
    cols = [
        c for c in fcast.columns if c.startswith(prefix) and c.endswith("_kt")
    ]
    long = fcast.melt(
        id_vars=["atcf_id", "issued_time", "leadtime_to_closest_h"],
        value_vars=cols,
        var_name="dept",
        value_name="wind_kt",
    )
    long["dept"] = long["dept"].str.slice(len(prefix), -3)
    return long.dropna(subset=["wind_kt"])


def _first_hit_lead(g: pd.DataFrame, flag, lead_col):
    hit = g[flag]
    return float(hit[lead_col].min()) if len(hit) else np.nan


def department_verdicts(
    rain_long: pd.DataFrame,
    wind_long: pd.DataFrame,
    storms: pd.DataFrame,
    dept_names: list[str],
) -> pd.DataFrame:
    """One row per (storm, department) with every orange reading.

    Lead times are hours from the first forecast meeting the criterion to
    the storm's closest approach - how much warning that reading gave.
    """
    thr_mm = dc.ORANGE_RAIN["threshold_mm"]
    closest = storms.set_index("atcf_id")["closest_time"]

    idx = pd.MultiIndex.from_product(
        [storms["atcf_id"].tolist(), dept_names], names=["atcf_id", "dept"]
    )
    out = pd.DataFrame(index=idx)

    if len(rain_long):
        r = rain_long.copy()
        r["issue_time"] = r["issue_date"] + pd.Timedelta(
            hours=dc.GEFS_ISSUE_HOUR_UTC
        )
        r["lead_h"] = (
            r["atcf_id"].map(closest) - r["issue_time"]
        ).dt.total_seconds() / 3600
        g = r.groupby(["atcf_id", "dept"])
        out["rain_mean_max_mm"] = g["mean_mm"].max()
        out["rain_pix_max_mm"] = g["pix_max_mm"].max()
        out["rain_mean_lead_h"] = g.apply(
            lambda d: _first_hit_lead(d, d["mean_mm"] >= thr_mm, "lead_h")
        )
        out["rain_pix_lead_h"] = g.apply(
            lambda d: _first_hit_lead(d, d["pix_max_mm"] >= thr_mm, "lead_h")
        )
        out["n_rain_fcasts"] = g.size()
    else:
        for c in (
            "rain_mean_max_mm",
            "rain_pix_max_mm",
            "rain_mean_lead_h",
            "rain_pix_lead_h",
        ):
            out[c] = np.nan
        out["n_rain_fcasts"] = 0

    g = wind_long.groupby(["atcf_id", "dept"])
    out["wind_max_kt"] = g["wind_kt"].max()
    out["wind_lead_h"] = g.apply(
        lambda d: _first_hit_lead(
            d, d["wind_kt"] >= dc.ORANGE_WIND_KT, "leadtime_to_closest_h"
        )
    )
    out["n_wind_fcasts"] = g.size()
    out = out.reset_index()
    out["n_rain_fcasts"] = out["n_rain_fcasts"].fillna(0).astype(int)
    out["n_wind_fcasts"] = out["n_wind_fcasts"].fillna(0).astype(int)
    out["wind_max_kmh"] = out["wind_max_kt"] * 1.852

    # Verdicts: NaN where the source has no forecast for that storm.
    def _flag(series, thr):
        return (series >= thr).where(series.notna())

    out["orange_wind"] = _flag(out["wind_max_kt"], dc.ORANGE_WIND_KT)
    out["orange_rain_mean"] = _flag(out["rain_mean_max_mm"], thr_mm)
    out["orange_rain_pix"] = _flag(out["rain_pix_max_mm"], thr_mm)
    for tag in ("mean", "pix"):
        w, r = out["orange_wind"], out[f"orange_rain_{tag}"]
        met = w.fillna(False).astype(bool) | r.fillna(False).astype(bool)
        # An OR is undetermined only when neither half is known to be met
        # and at least one half is unknown.
        unknown = ~met & (w.isna() | r.isna())
        out[f"orange_any_{tag}"] = met.where(~unknown)
        out[f"any_{tag}_lead_h"] = pd.concat(
            [out["wind_lead_h"], out[f"rain_{tag}_lead_h"]], axis=1
        ).max(axis=1)

    return out.merge(
        storms[["atcf_id", "name", "season", "closest_time"]],
        on="atcf_id",
        how="left",
    )


def storm_counts(verdicts: pd.DataFrame) -> pd.DataFrame:
    """Departments in orange per storm, under each reading."""
    rows = []
    for aid, g in verdicts.groupby("atcf_id", sort=False):
        rec = {
            "atcf_id": aid,
            "name": g["name"].iloc[0],
            "season": int(g["season"].iloc[0]),
            "rain_available": bool((g["n_rain_fcasts"] > 0).any()),
        }
        for k in READINGS:
            col = f"orange_{k}"
            known = g[col].notna()
            rec[f"n_dept_{k}"] = (
                int(g.loc[known, col].astype(bool).sum())
                if known.any()
                else np.nan
            )
            rec[f"depts_{k}"] = ", ".join(
                g.loc[known & g[col].fillna(False).astype(bool), "dept"]
            )
            lead_col = f"{k}_lead_h" if k != "wind" else "wind_lead_h"
            hit = g[g[col].fillna(False).astype(bool)]
            rec[f"lead_h_{k}"] = (
                float(hit[lead_col].max()) if len(hit) else np.nan
            )
        rows.append(rec)
    return pd.DataFrame(rows).sort_values("season").reset_index(drop=True)


def department_frequency(verdicts: pd.DataFrame) -> pd.DataFrame:
    """How often each department goes orange, per reading, with RP."""
    n_years = dc.SEASON_END - dc.SEASON_START + 1
    rows = []
    for dept, g in verdicts.groupby("dept", sort=False):
        rec = {"dept": dept}
        for k in READINGS:
            hit = g[g[f"orange_{k}"].fillna(False).astype(bool)]
            ny = hit["season"].nunique()
            rec[f"n_storms_{k}"] = len(hit)
            rec[f"n_years_{k}"] = ny
            rec[f"rp_{k}"] = (n_years + 1) / ny if ny else np.inf
        rows.append(rec)
    return pd.DataFrame(rows)
