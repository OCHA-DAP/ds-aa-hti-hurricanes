"""Reads from the storms schema (populated by ds-storms-pipeline).

All exposure/WSP tables live in the DEV database (the storms-pipeline
writes dev; see ds-knowledge-base pipelines/storms-pipeline.md).

Conventions:
- atcf_id is UPPERCASE in the DB ("AL132025"); the NHC blob CSVs used by
  the monitoring code are lowercase ("al132025"). Functions here take
  lowercase and convert.
- nhc_tracks_fcastonly_exposure is keyed by issued_time and covers the
  full (120 h) forecast horizon: future-only cone exposure, minus the
  observed swath already accumulated at that issuance.
- nhc_tracks_obsv_exposure is keyed by valid_time: exposure of the
  cumulative observed swath up to that time.
- WSP percentage is the LOWER edge of a probability band.
"""

import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd
from sqlalchemy import text

STAGE = "dev"


def naive_utc(ts) -> pd.Timestamp:
    """DB times are naive UTC; strip tz from aware inputs safely."""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def get_engine():
    return stratus.get_engine(stage=STAGE)


def fetch_fcastonly_exposure(atcf_id: str) -> pd.DataFrame:
    """Full-horizon fcastonly exposure per (issued_time, wind_speed_kt)."""
    query = text(
        """
        SELECT issued_time, wind_speed_kt, pop_exposed
        FROM storms.nhc_tracks_fcastonly_exposure
        WHERE iso3 = 'HTI' AND admin_level = 0 AND atcf_id = :atcf_id
        ORDER BY issued_time
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(query, conn, params={"atcf_id": atcf_id.upper()})


def fetch_obsv_exposure(atcf_id: str) -> pd.DataFrame:
    """Cumulative observed-swath exposure per (valid_time, wind_speed_kt)."""
    query = text(
        """
        SELECT valid_time, wind_speed_kt, pop_exposed
        FROM storms.nhc_tracks_obsv_exposure
        WHERE iso3 = 'HTI' AND admin_level = 0 AND atcf_id = :atcf_id
        ORDER BY valid_time
        """
    )
    with get_engine().connect() as conn:
        return pd.read_sql(query, conn, params={"atcf_id": atcf_id.upper()})


def fetch_tracks_geo(atcf_id: str, issued_time=None) -> gpd.GeoDataFrame:
    """NHC track points (observed + forecast) with leadtime and wind radii.

    issued_time=None returns observed points (leadtime IS NULL rows have
    issued_time == valid_time for observed provider rows).
    """
    params = {"atcf_id": atcf_id.upper()}
    where = "atcf_id = :atcf_id"
    if issued_time is not None:
        where += " AND issued_time = :issued_time"
        params["issued_time"] = naive_utc(issued_time)
    query = f"""
        SELECT atcf_id, provider, issued_time, valid_time, leadtime,
               wind_speed, quadrant_radius_34, quadrant_radius_50,
               quadrant_radius_64, geometry
        FROM storms.nhc_tracks_geo
        WHERE {where}
        ORDER BY valid_time
    """
    with get_engine().connect() as conn:
        return gpd.read_postgis(
            text(query), conn, params=params, geom_col="geometry"
        )


def fetch_wsp_exposure(atcf_id: str, issued_time=None) -> pd.DataFrame:
    """WSP probabilistic exposure bands for one storm.

    Returns [issued_time, wind_threshold_kt, percentage, pop_exposed],
    for the latest issuance <= issued_time if given, else the latest
    available.
    """
    params = {"atcf_id": atcf_id.upper()}
    where = "iso3 = 'HTI' AND admin_level = 0 AND atcf_id = :atcf_id"
    if issued_time is not None:
        where += " AND issued_time <= :issued_time"
        params["issued_time"] = naive_utc(issued_time)
    query = f"""
        WITH cand AS (
            SELECT issued_time, wind_threshold_kt, percentage, pop_exposed
            FROM storms.nhc_wsp_fcastonly_exposure
            WHERE {where}
        )
        SELECT * FROM cand
        WHERE issued_time = (SELECT MAX(issued_time) FROM cand)
        ORDER BY wind_threshold_kt, percentage
    """
    with get_engine().connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


def fetch_wsp_polygons(
    atcf_id: str, issued_time=None, wind_threshold_kt: int = 64
) -> gpd.GeoDataFrame:
    """WSP probability polygons for the map, one wind threshold."""
    params = {
        "atcf_id": atcf_id.upper(),
        "wind_threshold_kt": wind_threshold_kt,
    }
    where = "atcf_id = :atcf_id AND wind_threshold_kt = :wind_threshold_kt"
    if issued_time is not None:
        where += " AND issued_time <= :issued_time"
        params["issued_time"] = naive_utc(issued_time)
    query = f"""
        WITH cand AS (
            SELECT issued_time, percentage, geometry
            FROM storms.nhc_wsp_fcastonly_polygon
            WHERE {where}
        )
        SELECT * FROM cand
        WHERE issued_time = (SELECT MAX(issued_time) FROM cand)
        ORDER BY percentage
    """
    with get_engine().connect() as conn:
        return gpd.read_postgis(
            text(query), conn, params=params, geom_col="geometry"
        )


def fetch_hti_population() -> int:
    """HTI total population denominator (WorldPop, storms pipeline)."""
    query = text(
        """
        SELECT total_pop FROM storms.admin_population
        WHERE iso3 = 'HTI' AND admin_level = 0
        """
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    return int(df.iloc[0]["total_pop"]) if not df.empty else 11_757_597


def fetch_fcast_buffers(atcf_id: str, issued_time) -> gpd.GeoDataFrame:
    """Deterministic forecast wind buffers (34/50/64 kt) at one issuance."""
    query = text(
        """
        SELECT wind_speed_kt, geometry
        FROM storms.nhc_tracks_fcast_buffers
        WHERE atcf_id = :atcf_id AND issued_time = :issued_time
        ORDER BY wind_speed_kt
        """
    )
    with get_engine().connect() as conn:
        return gpd.read_postgis(
            query,
            conn,
            params={
                "atcf_id": atcf_id.upper(),
                "issued_time": naive_utc(issued_time),
            },
            geom_col="geometry",
        )


def fetch_obsv_buffer_at(atcf_id: str, at_time, wind_speed_kt: int = 64):
    """Cumulative observed wind-swath polygon at exactly at_time.

    The storms-pipeline fcastonly computation joins on an exact
    issued_time == valid_time match (no offset) and keeps the full
    forecast buffer when no swath exists yet; mirror that by returning
    (None, None) when absent.
    """
    query = text(
        """
        SELECT valid_time, geometry
        FROM storms.nhc_tracks_obsv_buffers
        WHERE atcf_id = :atcf_id
          AND wind_speed_kt = :wind_speed_kt
          AND valid_time = :at_time
        LIMIT 1
        """
    )
    with get_engine().connect() as conn:
        gdf = gpd.read_postgis(
            query,
            conn,
            params={
                "atcf_id": atcf_id.upper(),
                "wind_speed_kt": wind_speed_kt,
                "at_time": naive_utc(at_time),
            },
            geom_col="geometry",
        )
    if gdf.empty:
        return None, None
    return gdf.iloc[0]["valid_time"], gdf.iloc[0]["geometry"]


def fetch_storm_name(atcf_id: str) -> str:
    """Storm name from nhc_storms, falling back to ibtracs_storms.

    Some ingests store the literal string 'NaN' instead of NULL —
    treat those as missing too.
    """
    params = {"atcf_id": atcf_id.upper()}
    with get_engine().connect() as conn:
        for table in ("storms.nhc_storms", "storms.ibtracs_storms"):
            df = pd.read_sql(
                text(f"SELECT name FROM {table} WHERE atcf_id = :atcf_id"),
                conn,
                params=params,
            )
            for name in df["name"]:
                if pd.notnull(name) and str(name).lower() not in (
                    "nan",
                    "none",
                    "",
                ):
                    return str(name).capitalize()
    return atcf_id


def fetch_recent_issuances(start: pd.Timestamp) -> pd.DataFrame:
    """Atlantic NHC advisories since start: [atcf_id, issued_time].

    atcf_id is returned lowercase (monitoring convention).
    """
    query = text(
        """
        SELECT DISTINCT atcf_id, issued_time
        FROM storms.nhc_tracks_geo
        WHERE basin = 'NA' AND provider = 'nhc'
          AND leadtime IS NOT NULL
          AND issued_time >= :start
        ORDER BY issued_time
        """
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn, params={"start": naive_utc(start)})
    df["atcf_id"] = df["atcf_id"].str.lower()
    return df


def fetch_obsv_track(atcf_id: str) -> gpd.GeoDataFrame:
    """Observed track (leadtime-0 advisory positions) for one storm."""
    query = text(
        """
        SELECT atcf_id, issued_time AS valid_time, wind_speed, geometry
        FROM storms.nhc_tracks_geo
        WHERE atcf_id = :atcf_id AND provider = 'nhc' AND leadtime = 0
        ORDER BY issued_time
        """
    )
    with get_engine().connect() as conn:
        return gpd.read_postgis(
            query,
            conn,
            params={"atcf_id": atcf_id.upper()},
            geom_col="geometry",
        )
