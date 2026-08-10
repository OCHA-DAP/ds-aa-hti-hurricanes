"""Leadtime-capped forecast wind exposure for Haiti.

Replicates the ds-storms-pipeline fcastonly exposure computation
(buffer math imported from ocha-lens; zonal stats copied from
ds-storms-pipeline src/utils/exposure.py) with one modification: the
forecast track is capped at a maximum leadtime before buffering, so the
Action stage (<=72 h) can be evaluated separately from Mobilisation
(<=120 h, the full NHC horizon).

Faithfulness notes (deliberate, to match the storms.* tables):
- WorldPop 2026 1 km constrained (same raster as the pipeline).
- FieldMaps edge-matched adm0 boundary (not this repo's CODAB).
- Buffers built in EPSG:3857 pseudo-metres (radii ~5-6% small at
  Haiti's latitude), 30-min PCHIP track interpolation, quadrant radii
  bearing-interpolated - all inherited from ocha_lens.
- fcastonly = forecast buffer minus the cumulative observed swath at
  the same issuance (no offset), missing swath = keep full forecast.
"""

from functools import lru_cache

import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd
import xarray as xr
from ocha_lens.utils.storm import calculate_wind_buffers_gdf, expand_quad_col
from sqlalchemy import text

from src.constants import EXPOSURE_WIND_KT
from src.datasources import storms_db
from src.utils.logging import get_logger

logger = get_logger(__name__)

_POP_BLOB = "worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif"
_ADM0_BLOB = "fieldmaps/edge-matched/humanitarian/intl/adm0/HTI.parquet"


@lru_cache(maxsize=1)
def load_hti_adm0() -> gpd.GeoDataFrame:
    """FieldMaps edge-matched HTI adm0 (same boundary as storms-pipeline)."""
    gdf = stratus.load_geoparquet_from_blob(
        _ADM0_BLOB, stage="dev", container_name="global"
    )
    if len(gdf) > 1:
        gdf = gdf.dissolve()
    return gdf


@lru_cache(maxsize=1)
def load_pop_hti() -> xr.DataArray:
    """WorldPop 2026 1km clipped to the HTI adm0 window."""
    da = stratus.open_blob_cog(_POP_BLOB, container_name="raster").squeeze(
        drop=True
    )
    hti_geom = load_hti_adm0().geometry.union_all()
    return da.rio.clip([hti_geom], all_touched=True)


def calculate_exposure(
    gdf: gpd.GeoDataFrame,
    da: xr.DataArray,
    mask_geom=None,
    result_col: str = "pop_exposed",
) -> pd.DataFrame:
    """Population exposure per row in ``gdf``, using exactextract.

    Copied verbatim from ds-storms-pipeline src/utils/exposure.py so the
    numbers here match the storms.* exposure tables. Area-weighted sum
    of raster pixels per geometry (fractional edge pixels).
    """
    from exactextract import exact_extract
    from shapely.geometry import MultiPolygon, Polygon

    def _to_multipolygon(geom):
        # exactextract refuses a batch of mixed geometry types; the
        # forecast-minus-swath difference can leave Polygon for some
        # rows and MultiPolygon for others. Cast every row to
        # MultiPolygon, drop non-polygonal slivers.
        if geom is None or geom.is_empty:
            return geom
        if isinstance(geom, MultiPolygon):
            return geom
        if isinstance(geom, Polygon):
            return MultiPolygon([geom])
        polys = []
        for g in getattr(geom, "geoms", []):
            if isinstance(g, Polygon):
                polys.append(g)
            elif isinstance(g, MultiPolygon):
                polys.extend(g.geoms)
        return MultiPolygon(polys) if polys else None

    cols_out = [c for c in gdf.columns if c != gdf.geometry.name]
    if gdf.empty:
        return pd.DataFrame(columns=cols_out + [result_col])

    work = gdf.reset_index(drop=True).copy()
    if mask_geom is not None and not mask_geom.is_empty:
        work["geometry"] = work.geometry.intersection(mask_geom)
    work["geometry"] = work.geometry.apply(_to_multipolygon)

    valid = ~(work.geometry.is_empty | work.geometry.isna())
    out = work.loc[:, cols_out].copy()
    out[result_col] = 0

    if valid.any():
        sub = work.loc[valid]
        result = exact_extract(da, sub, ops=["sum"], output="pandas")
        out.loc[valid, result_col] = (
            result["sum"].fillna(0).round().astype("int64").values
        )

    return out


def fetch_fcast_track_points(
    atcf_id: str, issued_time: pd.Timestamp
) -> gpd.GeoDataFrame:
    """Forecast track points (with quadrant radii) for one issuance.

    Empty result means the storms-pipeline has not yet processed this
    advisory (defer the monitoring point) OR the storm genuinely has no
    forecast points; callers distinguish via `issuance_in_db`.
    """
    query = text(
        """
        SELECT atcf_id, basin, issued_time, valid_time, leadtime,
               wind_speed, quadrant_radius_34, quadrant_radius_50,
               quadrant_radius_64, geometry
        FROM storms.nhc_tracks_geo
        WHERE atcf_id = :atcf_id
          AND issued_time = :issued_time
          AND leadtime IS NOT NULL
        ORDER BY valid_time
        """
    )
    with storms_db.get_engine().connect() as conn:
        return gpd.read_postgis(
            query,
            conn,
            params={
                "atcf_id": atcf_id.upper(),
                "issued_time": storms_db.naive_utc(issued_time),
            },
            geom_col="geometry",
        )


def issuance_in_db(atcf_id: str, issued_time: pd.Timestamp) -> bool:
    """Has the storms-pipeline ingested this advisory into tracks_geo?"""
    query = text(
        """
        SELECT COUNT(*) AS n FROM storms.nhc_tracks_geo
        WHERE atcf_id = :atcf_id AND issued_time = :issued_time
        """
    )
    with storms_db.get_engine().connect() as conn:
        n = pd.read_sql(
            query,
            conn,
            params={
                "atcf_id": atcf_id.upper(),
                "issued_time": storms_db.naive_utc(issued_time),
            },
        ).iloc[0]["n"]
    return n > 0


def fcastonly_exposure(
    atcf_id: str,
    issued_time: pd.Timestamp,
    lt_cap_hrs: int,
    wind_speed_kt: int = EXPOSURE_WIND_KT,
) -> float | None:
    """Population exposed to >= wind_speed_kt forecast winds within
    lt_cap_hrs leadtime, minus the already-observed swath.

    Returns None when the advisory is not yet in the DB (caller should
    defer); 0.0 when computed and there is no overlap with Haiti.
    """
    gdf = fetch_fcast_track_points(atcf_id, issued_time)
    if gdf.empty:
        if not issuance_in_db(atcf_id, issued_time):
            return None
        return 0.0

    gdf = gdf[gdf["leadtime"] <= lt_cap_hrs]
    has_radii = (
        gdf[
            [
                "quadrant_radius_34",
                "quadrant_radius_50",
                "quadrant_radius_64",
            ]
        ]
        .notna()
        .any(axis=1)
    )
    gdf = gdf[has_radii]
    if gdf.empty:
        return 0.0

    for speed in (34, 50, 64):
        gdf = expand_quad_col(gdf, f"quadrant_radius_{speed}")
    buffers = calculate_wind_buffers_gdf(
        gdf, quad_cols_format="quadrant_radius_{speed}_{quad}"
    )
    buf_geom = buffers.set_index("wind_speed_kt").loc[
        wind_speed_kt, "geometry"
    ]
    if buf_geom is None or buf_geom.is_empty:
        return 0.0

    # subtract the cumulative observed swath at this issuance
    _, obsv_geom = storms_db.fetch_obsv_buffer_at(
        atcf_id, issued_time, wind_speed_kt
    )
    if obsv_geom is not None:
        buf_geom = buf_geom.difference(obsv_geom)
        if buf_geom.is_empty:
            return 0.0

    hti_geom = load_hti_adm0().geometry.union_all()
    if not buf_geom.intersects(hti_geom):
        return 0.0
    df = calculate_exposure(
        gpd.GeoDataFrame(geometry=[buf_geom], crs="EPSG:4326"),
        load_pop_hti(),
        mask_geom=hti_geom,
    )
    return float(df["pop_exposed"].iloc[0])


def obsv_exposure_at(
    atcf_id: str,
    at_time: pd.Timestamp,
    wind_speed_kt: int = EXPOSURE_WIND_KT,
) -> float:
    """Cumulative observed exposure at/before at_time (0 if none)."""
    df = storms_db.fetch_obsv_exposure(atcf_id)
    df = df[
        (df["wind_speed_kt"] == wind_speed_kt)
        & (df["valid_time"] <= storms_db.naive_utc(at_time))
    ]
    if df.empty:
        return 0.0
    return float(df.sort_values("valid_time").iloc[-1]["pop_exposed"])
