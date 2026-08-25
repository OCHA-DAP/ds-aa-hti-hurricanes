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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# A storm window must arrive essentially complete; see fetch_window.
MIN_GRANULE_FRACTION = 0.98

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


class EarthdataSession(requests.Session):
    """Session that survives Earthdata's cross-host login redirect.

    A GES DISC data URL bounces through ``urs.earthdata.nasa.gov`` and back.
    ``requests`` strips the Authorization header on any cross-host redirect
    (by design), so a plain ``session.auth`` never reaches the login host
    and every fetch returns "Credentials ... are invalid". This is NASA's
    documented workaround: keep the header for the Earthdata hosts in the
    chain, drop it for anything else.
    """

    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        if "Authorization" not in headers:
            return
        original = requests.utils.urlparse(response.request.url).hostname
        redirect = requests.utils.urlparse(prepared_request.url).hostname
        if (
            original != redirect
            and redirect != self.AUTH_HOST
            and original != self.AUTH_HOST
        ):
            del headers["Authorization"]


def _session():
    """Authenticated Earthdata session (also writes the .netrc/.dodsrc set).

    Missing credentials fail loudly here rather than as 14 000 identical
    "invalid credentials" granule warnings.
    """
    user = os.environ.get("IMERG_USERNAME")
    pw = os.environ.get("IMERG_PASSWORD")
    if not user or not pw:
        raise RuntimeError(
            "IMERG_USERNAME / IMERG_PASSWORD are not set. On Databricks "
            "they come from the cluster policy (secret scope `dsci`); "
            "locally they come from .env."
        )
    try:
        from src.datasources.imerg import create_auth_files

        create_auth_files()
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not write Earthdata auth files: %s", exc)

    session = EarthdataSession(user, pw)
    # GES DISC throttles bursts with 503s. Back off and retry rather than
    # dropping granules — an incomplete window is useless to us.
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=32)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _fetch_one(session, filename, run, bbox):
    ilon0, ilon1, ilat0, ilat1 = _bbox_indices(bbox)
    ce = (
        f"precipitation[0:0][{ilon0}:{ilon1}][{ilat0}:{ilat1}],"
        f"lon[{ilon0}:{ilon1}],lat[{ilat0}:{ilat1}],time[0:0]"
    )
    url = f"{_opendap_url(filename, run)}.nc4?{ce}"
    r = session.get(url, timeout=180, allow_redirects=True)
    r.raise_for_status()
    head = r.content[:4]
    if head[:3] == b"CDF":
        return xr.open_dataset(BytesIO(r.content))
    if head == b"\x89HDF":
        # OPeNDAP's .nc4 response is HDF5, which xarray can only open
        # through h5netcdf (the netcdf4 backend declines it here).
        return xr.open_dataset(BytesIO(r.content), engine="h5netcdf")
    raise RuntimeError(
        f"non-netCDF response for {filename}: {r.content[:200]!r}"
    )


def fetch_window(start, end, run="final", bbox=HTI_BBOX, max_workers=4):
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

    # A window is only usable if nearly all of it arrived: a rolling
    # accumulation over a series with holes silently *understates* the
    # maximum, which would read as "threshold not met" rather than as an
    # error. Fail loudly instead.
    got, want = len(frames), len(granules)
    if got < want:
        logger.warning("%d of %d granules failed", want - got, want)
    if got < MIN_GRANULE_FRACTION * want:
        raise RuntimeError(
            f"only {got}/{want} granules fetched "
            f"({got / want:.0%}); refusing to compute accumulations from "
            "an incomplete series (check Earthdata credentials)"
        )

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
