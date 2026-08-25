"""Haiti analysis grid: land mask, department labels, population weights.

Built on the WorldPop 2026 1 km raster the framework already uses for
exposure, so wind results here are directly comparable with the existing
64-kt exposure trigger.
"""

from functools import lru_cache

import geopandas as gpd
import numpy as np
import xarray as xr
from rasterio.features import rasterize

from src.datasources import codab
from src.monitoring.exposure import load_pop_hti


@lru_cache(maxsize=1)
def load_grid():
    """Return (lat2d, lon2d, land_mask, pop, dept_idx, dept_names).

    ``dept_idx`` is -1 off land, otherwise an index into ``dept_names``.
    """
    pop: xr.DataArray = load_pop_hti()
    lons = pop["x"].to_numpy()
    lats = pop["y"].to_numpy()
    lon2d, lat2d = np.meshgrid(lons, lats)

    raw = pop.to_numpy()
    # WorldPop carries an explicit nodata fill (-99999), not NaN, and
    # rio.clip writes that fill outside the adm0 boundary.
    nodata = pop.rio.nodata
    land = np.isfinite(raw)
    if nodata is not None:
        land &= raw != nodata
    pop_vals = np.where(land, raw, 0.0)

    adm1: gpd.GeoDataFrame = codab.load_codab_from_blob(admin_level=1).to_crs(
        pop.rio.crs
    )
    adm1 = adm1.sort_values("ADM1_PCODE").reset_index(drop=True)
    dept_names = adm1["ADM1_FR"].tolist()

    dept_idx = rasterize(
        ((geom, i) for i, geom in enumerate(adm1.geometry)),
        out_shape=pop.shape,
        transform=pop.rio.transform(),
        fill=-1,
        all_touched=True,
        dtype="int32",
    )
    # Restrict department labels to the land mask, and treat any land cell
    # the rasteriser missed as belonging to its nearest labelled neighbour
    # by leaving it at -1 (excluded from department stats, kept in national).
    dept_idx = np.where(land, dept_idx, -1)

    return lat2d, lon2d, land, pop_vals, dept_idx, dept_names


def aggregate(field, land, pop, dept_idx, n_depts):
    """Three spatial aggregations of a gridded field over Haiti.

    Returns a dict with ``any_pixel`` (max over land), ``national_mean``
    (unweighted mean over land), ``department_max`` (max over departments
    of the department mean) and the per-department means.
    """
    vals = np.where(land, field, np.nan)
    dept_means = np.full(n_depts, np.nan)
    for i in range(n_depts):
        sel = dept_idx == i
        if sel.any():
            dept_means[i] = np.nanmean(np.where(sel, field, np.nan))

    return {
        "any_pixel": float(np.nanmax(vals)) if land.any() else np.nan,
        "national_mean": float(np.nanmean(vals)) if land.any() else np.nan,
        "department_max": (
            float(np.nanmax(dept_means))
            if np.isfinite(dept_means).any()
            else np.nan
        ),
        "dept_means": dept_means,
    }


def pop_above(field, land, pop, threshold):
    """Population on land where ``field`` >= ``threshold``."""
    sel = land & (field >= threshold)
    return float(pop[sel].sum())
