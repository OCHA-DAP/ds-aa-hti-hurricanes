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


def selfcheck(verbose=True):
    """Assert the reduction's invariants.

    Run: ``python -m src.dgpc.rain_analysis``.

    Injects a known extreme at a real land point (Les Cayes) over otherwise
    light noise and checks that:
    1. a point extreme surfaces in ``any_pixel`` and is diluted away in the
       department and national means (the aggregation choice is the single
       biggest lever on the DGPC verdict, so it must demonstrably bite);
    2. the ordering any_pixel >= department_max >= national_mean holds at
       every window;
    3. sea cells are excluded - Haiti is horseshoe-shaped, so the centre of
       its bounding box is open water in the Golfe de la Gonave.
    """
    import numpy as _np
    import pandas as _pd
    import xarray as _xr

    from src.datasources.imerg_hh import GRID_RES, HTI_BBOX

    minx, miny, maxx, maxy = HTI_BBOX
    lons = _np.arange(minx, maxx, GRID_RES) + GRID_RES / 2
    lats = _np.arange(miny, maxy, GRID_RES) + GRID_RES / 2
    times = _pd.date_range("2016-10-03", periods=48 * 5, freq="30min")

    rng = _np.random.default_rng(0)
    vals = rng.gamma(0.3, 0.4, size=(len(times), len(lats), len(lons)))
    ci = int(_np.abs(lats - 18.20).argmin())  # Les Cayes, Sud
    cj = int(_np.abs(lons - (-73.75)).argmin())
    vals[100:104, ci, cj] = 40.0  # 80 mm in any 1 h
    vals[200:248, :, :] += 2.5  # broad 24 h soak

    da = _xr.DataArray(
        vals,
        coords={"time": times, "lat": lats, "lon": lons},
        dims=("time", "lat", "lon"),
    )
    stats = storm_rain_stats(da)
    crit = evaluate_criteria(stats)

    failures = []
    if not stats["any_pixel_1h_mm"] > 75:
        failures.append(
            f"any_pixel 1h = {stats['any_pixel_1h_mm']:.1f}, want >75"
        )
    if not crit["any_pixel_rain_red_rate"]:
        failures.append("60 mm/h should trip on any_pixel")
    if crit["national_mean_rain_red_rate"]:
        failures.append(
            "a single-cell extreme must not trip the national mean"
        )
    for w in WINDOWS:
        a, d, n = (
            stats[f"any_pixel_{w}_mm"],
            stats[f"department_max_{w}_mm"],
            stats[f"national_mean_{w}_mm"],
        )
        if not (a >= d >= n):
            failures.append(f"{w}: ordering broken ({a:.1f}/{d:.1f}/{n:.1f})")
        if verbose:
            print(
                f"  {w:>3}: national {n:7.1f} | dept_max {d:7.1f} "
                f"| any_pixel {a:7.1f}"
            )

    # Sea cells excluded: the bbox centre sits in the Golfe de la Gonave.
    from src.utils.raster import upsample_dataarray

    up = upsample_dataarray(
        da.isel(time=slice(0, 2)),
        resolution=UPSAMPLE_RES,
        lat_dim="lat",
        lon_dim="lon",
    )
    land, _, _ = _masks(up)
    mid_lat = int(_np.abs(up.lat.to_numpy() - 19.10).argmin())
    mid_lon = int(_np.abs(up.lon.to_numpy() - (-73.05)).argmin())
    if bool(land[mid_lat, mid_lon]):
        failures.append("Golfe de la Gonave is being counted as land")

    if failures:
        raise AssertionError(
            "rain_analysis selfcheck failed:\n  " + "\n  ".join(failures)
        )
    if verbose:
        print("rain_analysis selfcheck: OK")
    return True


if __name__ == "__main__":
    selfcheck()
