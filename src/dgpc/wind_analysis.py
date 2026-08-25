"""Run the parametric wind field over every storm in the Haiti set.

Produces, per storm:
- the maximum wind reaching Haitian land, from NHC forecasts (per issuance,
  then the per-storm maximum) and from the IBTrACS best track;
- per-department maxima and population above each DGPC wind level;
- all three wind readings (sustained over land, unreduced, gust).
"""

import geopandas as gpd
import numpy as np
import pandas as pd
from sqlalchemy import text

from src.constants import D_THRESH
from src.datasources import codab
from src.datasources import storms_db as sdb
from src.dgpc import constants as dc
from src.dgpc.grid import load_grid, pop_above
from src.dgpc.windfield import haversine_km, interpolate_track, max_wind_field
from src.utils.logging import get_logger

logger = get_logger(__name__)

FCAST_Q = """
SELECT atcf_id, issued_time, valid_time, leadtime, wind_speed,
       quadrant_radius_34, quadrant_radius_50, quadrant_radius_64,
       ST_X(geometry) lon, ST_Y(geometry) lat
FROM storms.nhc_tracks_geo
WHERE provider = 'nhc' AND atcf_id = :a
ORDER BY issued_time, leadtime
"""

OBSV_Q = """
SELECT t.sid, t.valid_time, t.wind_speed, t.max_wind_radius,
       t.usa_quadrant_radius_34 AS quadrant_radius_34,
       t.usa_quadrant_radius_50 AS quadrant_radius_50,
       t.usa_quadrant_radius_64 AS quadrant_radius_64,
       ST_X(t.geometry) lon, ST_Y(t.geometry) lat
FROM storms.ibtracs_tracks_geo t
JOIN storms.ibtracs_storms s ON s.sid = t.sid
WHERE s.atcf_id = :a AND t.provider IN ('hurdat_atl', 'nhc_working_bt', 'atcf')
ORDER BY t.valid_time
"""


def storm_set():
    """Storms whose analysed centre came within D_THRESH km of Haiti."""
    q = """
    SELECT atcf_id, valid_time, wind_speed,
           ST_X(geometry) lon, ST_Y(geometry) lat
    FROM storms.nhc_tracks_geo
    WHERE provider = 'nhc' AND leadtime = 0
      AND date_part('year', valid_time) BETWEEN :s AND :e
    """
    e = sdb.get_engine()
    df = pd.read_sql(
        text(q), e, params={"s": dc.SEASON_START, "e": dc.SEASON_END}
    )
    adm0 = codab.load_codab_from_blob(admin_level=0)
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.lon, df.lat), crs=4326
    ).to_crs(3857)
    hti = adm0.to_crs(3857).geometry.union_all()
    gdf["dist_km"] = gdf.distance(hti) / 1000
    near = gdf[gdf.dist_km <= D_THRESH]

    storms = (
        near.groupby("atcf_id")
        .agg(
            min_dist_km=("dist_km", "min"),
            closest_time=("valid_time", lambda s: s.loc[s.index[0]]),
            first_near=("valid_time", "min"),
            last_near=("valid_time", "max"),
        )
        .reset_index()
    )
    idx = near.groupby("atcf_id")["dist_km"].idxmin()
    storms["closest_time"] = near.loc[idx, "valid_time"].to_numpy()

    storms = resolve_names(storms)
    storms["season"] = (
        storms["season"].fillna(storms.first_near.dt.year).astype(int)
    )
    return storms.sort_values("first_near").reset_index(drop=True)


def resolve_names(df: pd.DataFrame) -> pd.DataFrame:
    """Attach storm names, coalescing both catalogues.

    ``storms.nhc_storms`` is missing names for recent seasons (2025's
    Imelda and Melissa among them), so fall back to ``ibtracs_storms``.
    """
    e = sdb.get_engine()
    out = df.drop(
        columns=[c for c in ("name", "season") if c in df], errors="ignore"
    )
    nhc = pd.read_sql(
        text("SELECT atcf_id, name, season FROM storms.nhc_storms"), e
    )
    ibt = pd.read_sql(
        text("SELECT atcf_id, name, season FROM storms.ibtracs_storms"), e
    ).rename(columns={"name": "name_ibt", "season": "season_ibt"})

    out = out.merge(nhc, on="atcf_id", how="left").merge(
        ibt, on="atcf_id", how="left"
    )
    # Parquet round-trips a missing name as the literal string "NaN" under
    # the pandas string dtype, so test the text, not just null-ness.
    blank = out["name"].isna() | out["name"].astype(
        str
    ).str.strip().str.lower().isin(["", "nan", "none", "unnamed"])
    out["name"] = out["name"].where(~blank, out["name_ibt"])
    out["season"] = out["season"].fillna(out["season_ibt"])
    return out.drop(columns=["name_ibt", "season_ibt"])


def _variants(field_kt):
    """The three wind readings, from the unreduced (marine) field."""
    land = field_kt * dc.LAND_REDUCTION_FACTOR
    return {
        "sustained_marine": field_kt,
        "sustained_land": land,
        "gust_land": land * dc.GUST_FACTOR,
    }


def _summarise(field_kt, land, pop, dept_idx, dept_names):
    """Per-variant summary of one maximum-wind field."""
    out = {}
    for vname, fld in _variants(field_kt).items():
        vals = np.where(land, fld, np.nan)
        if not np.isfinite(vals).any():
            continue
        out[f"{vname}_max_kt"] = float(np.nanmax(vals))
        out[f"{vname}_max_kmh"] = float(np.nanmax(vals)) * 1.852
        out[f"{vname}_pop_orange"] = pop_above(
            fld, land, pop, dc.ORANGE_WIND_KT
        )
        out[f"{vname}_pop_red"] = pop_above(fld, land, pop, dc.RED_WIND_KT)
        dept_max = {}
        for i, nm in enumerate(dept_names):
            sel = dept_idx == i
            if sel.any():
                dept_max[nm] = float(np.nanmax(np.where(sel, fld, np.nan)))
        for nm, v in dept_max.items():
            out[f"{vname}_dept_{nm}_kt"] = v
        out[f"{vname}_n_dept_orange"] = sum(
            1 for v in dept_max.values() if v >= dc.ORANGE_WIND_KT
        )
        out[f"{vname}_n_dept_red"] = sum(
            1 for v in dept_max.values() if v >= dc.RED_WIND_KT
        )
    return out


def forecast_wind_by_issuance(atcf_id, rmw_scale=1.0, max_dist_km=600):
    """Max wind on Haitian land for each NHC forecast issuance."""
    lat2d, lon2d, land, pop, dept_idx, dept_names = load_grid()
    hti_lat, hti_lon = float(np.mean(lat2d)), float(np.mean(lon2d))

    df = pd.read_sql(text(FCAST_Q), sdb.get_engine(), params={"a": atcf_id})
    if df.empty:
        return pd.DataFrame()

    records = []
    for issued, grp in df.groupby("issued_time"):
        # Only issuances that bring the storm near Haiti at some point.
        d = haversine_km(
            grp.lat.to_numpy(), grp.lon.to_numpy(), hti_lat, hti_lon
        )
        if d.min() > max_dist_km:
            continue
        track = interpolate_track(grp, step_hours=1.0)
        field = max_wind_field(
            track, lat2d, lon2d, max_dist_km=800, rmw_scale=rmw_scale
        )
        rec = {
            "atcf_id": atcf_id,
            "issued_time": issued,
            "min_fcast_dist_km": float(d.min()),
            "max_fcast_vmax_kt": float(grp.wind_speed.max()),
            "leadtime_to_closest_h": float(
                grp.loc[grp.index[int(np.argmin(d))], "leadtime"]
            ),
        }
        rec.update(_summarise(field, land, pop, dept_idx, dept_names))
        records.append(rec)

    return pd.DataFrame(records)


def observed_wind(atcf_id, rmw_scale=1.0, closest_time=None):
    """Max wind on Haitian land from the IBTrACS best track.

    ``closest_time`` (the storm's closest approach) is used to check that
    the best track actually spans the approach. IBTrACS starts a track when
    a system is named, so a storm that passed Haiti before naming — Hermine
    2016 is three days short — has no observed wind field there at all.
    Reporting the resulting 0 km/h as an observation would be wrong, so the
    gap is flagged and the page shows it as undetermined.
    """
    lat2d, lon2d, land, pop, dept_idx, dept_names = load_grid()
    df = pd.read_sql(text(OBSV_Q), sdb.get_engine(), params={"a": atcf_id})
    if df.empty:
        return {"atcf_id": atcf_id, "obsv_track_covers": False}

    df = df.rename(columns={"valid_time": "vt"})
    df["leadtime"] = (df.vt - df.vt.min()).dt.total_seconds() / 3600.0
    df["issued_time"] = df.vt.min()
    df["atcf_id"] = atcf_id
    track = interpolate_track(df, step_hours=1.0)
    field = max_wind_field(
        track, lat2d, lon2d, max_dist_km=800, rmw_scale=rmw_scale
    )
    covers = True
    if closest_time is not None:
        ct = pd.Timestamp(closest_time)
        tol = pd.Timedelta(hours=3)
        covers = bool(
            (df["vt"].min() - tol) <= ct <= (df["vt"].max() + tol)
        )

    rec = {
        "atcf_id": atcf_id,
        "obsv_vmax_kt": float(df.wind_speed.max()),
        "obsv_track_covers": covers,
    }
    rec.update(_summarise(field, land, pop, dept_idx, dept_names))
    return rec
