"""DGPC rainfall criteria evaluated on IMERG half-hourly precipitation.

For each storm the half-hourly field is reduced to the maximum rolling
accumulation over each DGPC window (1 h, 6 h, 12 h, 24 h) under three
spatial aggregations:

- ``national_mean``   - country mean series, then rolled
- ``department_max``  - each department's mean series rolled, then the
                        largest department
- ``any_pixel``       - rolled per grid cell, then the largest cell

Order matters: rolling a mean is not the mean of rollings. The first two
aggregate before rolling, the third after.

Resolution caveat: IMERG cells are 0.1 degrees (~11 km). ``any_pixel`` is
therefore an ~11 km cell average, not a point measurement, and gridded
satellite estimates systematically smooth extreme short-duration rates.
The 60-80 mm/h red criterion is the one most affected.
"""

import numpy as np
import pandas as pd
import xarray as xr
from rasterio.features import rasterize

from src.datasources import codab
from src.dgpc import constants as dc

# Half-hourly steps per DGPC window.
WINDOWS = {"1h": 2, "6h": 12, "12h": 24, "24h": 48}
UPSAMPLE_RES = 0.05


def _masks(da: xr.DataArray):
    """Land mask and department index on the (upsampled) IMERG grid."""
    adm0 = codab.load_codab_from_blob(admin_level=0)
    adm1 = codab.load_codab_from_blob(admin_level=1).sort_values("ADM1_PCODE")
    dept_names = adm1["ADM1_FR"].tolist()

    lats = da["lat"].to_numpy()
    lons = da["lon"].to_numpy()
    res_y = float(lats[1] - lats[0])
    res_x = float(lons[1] - lons[0])
    from rasterio.transform import from_origin

    transform = from_origin(
        lons.min() - res_x / 2, lats.max() + abs(res_y) / 2, res_x, abs(res_y)
    )
    shape = (len(lats), len(lons))
    flip = res_y > 0  # rasterize assumes north-up

    land = rasterize(
        [(adm0.geometry.union_all(), 1)],
        out_shape=shape,
        transform=transform,
        fill=0,
        all_touched=True,
        dtype="uint8",
    ).astype(bool)
    dept = rasterize(
        ((g, i) for i, g in enumerate(adm1.geometry)),
        out_shape=shape,
        transform=transform,
        fill=-1,
        all_touched=True,
        dtype="int32",
    )
    if flip:
        land = land[::-1]
        dept = dept[::-1]
    return land, np.where(land, dept, -1), dept_names


def _rolling_max(series: pd.Series, steps: int) -> float:
    """Largest rolling sum of ``steps`` consecutive half-hours."""
    if len(series) < steps:
        return float("nan")
    return float(series.rolling(steps, min_periods=steps).sum().max())


def storm_rain_stats(da: xr.DataArray) -> dict:
    """All windows x all aggregations for one storm's half-hourly field.

    ``da`` has dims (time, lat, lon) in mm per half hour.
    """
    from src.utils.raster import upsample_dataarray

    da = upsample_dataarray(
        da, resolution=UPSAMPLE_RES, lat_dim="lat", lon_dim="lon"
    )
    land, dept_idx, dept_names = _masks(da)
    vals = da.to_numpy()  # (time, lat, lon)
    times = pd.DatetimeIndex(da["time"].to_numpy())

    land_da = xr.DataArray(land, dims=("lat", "lon"))
    nat_series = pd.Series(
        da.where(land_da).mean(dim=("lat", "lon")).to_numpy(), index=times
    )

    dept_series = {}
    for i, nm in enumerate(dept_names):
        m = xr.DataArray(dept_idx == i, dims=("lat", "lon"))
        if not bool(m.any()):
            continue
        dept_series[nm] = pd.Series(
            da.where(m).mean(dim=("lat", "lon")).to_numpy(), index=times
        )

    out = {}
    for wname, steps in WINDOWS.items():
        out[f"national_mean_{wname}_mm"] = _rolling_max(nat_series, steps)

        d_max = {nm: _rolling_max(s, steps) for nm, s in dept_series.items()}
        for nm, v in d_max.items():
            out[f"dept_{nm}_{wname}_mm"] = v
        finite = [v for v in d_max.values() if np.isfinite(v)]
        out[f"department_max_{wname}_mm"] = max(finite) if finite else np.nan

        # any_pixel: roll each cell, then take the largest cell.
        if len(times) >= steps:
            k = np.ones(steps)
            rolled = np.apply_along_axis(
                lambda c: np.convolve(np.nan_to_num(c), k, mode="valid"),
                0,
                vals,
            )
            masked = np.where(land[None, :, :], rolled, np.nan)
            out[f"any_pixel_{wname}_mm"] = (
                float(np.nanmax(masked))
                if np.isfinite(masked).any()
                else np.nan
            )
        else:
            out[f"any_pixel_{wname}_mm"] = np.nan

    return out


def evaluate_criteria(stats: dict) -> dict:
    """Which DGPC rainfall conditions the stored maxima satisfy."""
    out = {}
    for agg in dc.AGGREGATIONS:
        p = "department_max" if agg == "department_max" else agg
        orange = (
            stats.get(f"{p}_24h_mm", np.nan) >= dc.ORANGE_RAIN["threshold_mm"]
        )
        red_rate = (
            stats.get(f"{p}_1h_mm", np.nan) >= dc.RED_RAIN_RATE["threshold_mm"]
        )
        red_12h = (
            stats.get(f"{p}_12h_mm", np.nan)
            >= dc.RED_RAIN_ACCUM["threshold_mm"]
        )
        red_6h = (
            stats.get(f"{p}_6h_mm", np.nan)
            >= dc.RED_RAIN_ACCUM_STRICT["threshold_mm"]
        )
        out[f"{agg}_rain_orange"] = bool(orange)
        out[f"{agg}_rain_red_rate"] = bool(red_rate)
        out[f"{agg}_rain_red_12h"] = bool(red_12h)
        out[f"{agg}_rain_red_6h"] = bool(red_6h)
        out[f"{agg}_rain_red"] = bool(red_rate or red_12h)
    return out


def storm_window(first_near, last_near, pad_days=2):
    """Rainfall attribution window for a storm."""
    return (
        pd.Timestamp(first_near) - pd.Timedelta(days=pad_days),
        pd.Timestamp(last_near) + pd.Timedelta(days=pad_days),
    )
