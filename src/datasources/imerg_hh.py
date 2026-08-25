"""IMERG half-hourly precipitation over Haiti.

The framework's existing IMERG plumbing (``src/datasources/imerg.py``) is
daily, which cannot address the DGPC sub-daily criteria (60-80 mm/h,
300 mm/6-12 h). This module pulls the half-hourly product instead.

Granules are discovered through NASA's CMR search API (no authentication)
and fetched one at a time from the GES DISC OPeNDAP endpoint with an
index-space bounding-box constraint, so each response is a few kB rather
than a 30 MB global grid. Earthdata credentials are still needed for the
data fetch itself (``IMERG_USERNAME`` / ``IMERG_PASSWORD``).

The ``precipitation`` variable is a *rate* in mm/h valid over the
half-hour, so the accumulation contributed by one granule is rate / 2.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

import numpy as np
import pandas as pd
import requests
import xarray as xr

from src.utils.logging import get_logger

logger = get_logger(__name__)

CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
OPENDAP_ROOT = "https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L3"

# IMERG grid geometry (0.1 degree, cell centres at -179.95 / -89.95 + 0.1k).
GRID_ORIGIN_LON = -180.0
GRID_ORIGIN_LAT = -90.0
GRID_RES = 0.1

# Haiti with a small pad, so edge cells are not clipped away.
HTI_BBOX = (-74.6, 17.9, -71.5, 20.2)

COLLECTIONS = {
    "final": ("GPM_3IMERGHH", "GPM_3IMERGHH.07"),
    "late": ("GPM_3IMERGHHL", "GPM_3IMERGHHL.07"),
}


def _bbox_indices(bbox=HTI_BBOX):
    minx, miny, maxx, maxy = bbox
    ilon0 = int(np.floor((minx - GRID_ORIGIN_LON) / GRID_RES))
    ilon1 = int(np.ceil((maxx - GRID_ORIGIN_LON) / GRID_RES))
    ilat0 = int(np.floor((miny - GRID_ORIGIN_LAT) / GRID_RES))
    ilat1 = int(np.ceil((maxy - GRID_ORIGIN_LAT) / GRID_RES))
    return ilon0, ilon1, ilat0, ilat1


def find_granules(start, end, run="final"):
    """Granule filenames and times for a window, via CMR (no auth)."""
    short_name, _ = COLLECTIONS[run]
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    out, page = [], 1
    while True:
        r = requests.get(
            CMR_URL,
            params={
                "short_name": short_name,
                "version": "07",
                "temporal": (
                    f"{start:%Y-%m-%dT%H:%M:%SZ},{end:%Y-%m-%dT%H:%M:%SZ}"
                ),
                "page_size": 2000,
                "page_num": page,
            },
            timeout=120,
        )
        r.raise_for_status()
        entries = r.json()["feed"]["entry"]
        if not entries:
            break
        for e in entries:
            title = e["title"].split(":")[-1]
            out.append(
                (pd.Timestamp(e["time_start"]).tz_localize(None), title)
            )
        if len(entries) < 2000:
            break
        page += 1
    return sorted(set(out))


def _opendap_url(filename, run="final"):
    _, coll = COLLECTIONS[run]
    # 3B-HHR.MS.MRG.3IMERG.20161004-S000000-E002959.0000.V07B.HDF5
    datestr = filename.split(".3IMERG.")[1][:8]
    ts = pd.Timestamp(datestr)
    return f"{OPENDAP_ROOT}/{coll}/{ts.year}/{ts.dayofyear:03d}/{filename}"


def _session():
    s = requests.Session()
    user = os.environ["IMERG_USERNAME"]
    pw = os.environ["IMERG_PASSWORD"]
    s.auth = (user, pw)
    return s


def _fetch_one(session, filename, run, bbox):
    ilon0, ilon1, ilat0, ilat1 = _bbox_indices(bbox)
    ce = (
        f"precipitation[0:0][{ilon0}:{ilon1}][{ilat0}:{ilat1}],"
        f"lon[{ilon0}:{ilon1}],lat[{ilat0}:{ilat1}],time[0:0]"
    )
    url = f"{_opendap_url(filename, run)}.nc4?{ce}"
    r = session.get(url, timeout=180, allow_redirects=True)
    r.raise_for_status()
    if r.content[:3] == b"\x89HD" or r.content[:3] == b"CDF":
        return xr.open_dataset(BytesIO(r.content))
    raise RuntimeError(
        f"non-netCDF response for {filename}: {r.content[:200]!r}"
    )


def fetch_window(start, end, run="final", bbox=HTI_BBOX, max_workers=8):
    """Half-hourly precipitation *accumulation* (mm) over the window.

    Returns a DataArray with dims (time, lat, lon); each time step is the
    millimetres that fell in that half hour.
    """
    granules = find_granules(start, end, run=run)
    if not granules:
        raise RuntimeError(f"no {run} IMERG granules for {start} - {end}")
    logger.info(
        "fetching %d half-hourly granules (%s run)", len(granules), run
    )

    session = _session()
    frames = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {
            ex.submit(_fetch_one, session, fn, run, bbox): (ts, fn)
            for ts, fn in granules
        }
        for fut in as_completed(futs):
            ts, fn = futs[fut]
            try:
                frames[ts] = fut.result()
            except Exception as exc:
                logger.warning("granule %s failed: %s", fn, exc)

    if not frames:
        raise RuntimeError("every granule fetch failed (check credentials)")

    times = sorted(frames)
    arrs = []
    for ts in times:
        ds = frames[ts]
        da = ds["precipitation"].squeeze(drop=True)
        # OPeNDAP serves IMERG as (lon, lat); transpose to (lat, lon).
        if da.dims[0] == "lon":
            da = da.transpose("lat", "lon")
        arrs.append(da)

    out = xr.concat(arrs, dim=pd.DatetimeIndex(times, name="time"))
    out = out.where(out >= 0)  # -9999.9 fill
    out = out / 2.0  # mm/h rate over a half hour -> mm
    out.attrs["units"] = "mm per half hour"
    out.attrs["run"] = run
    return out.sortby("time")
