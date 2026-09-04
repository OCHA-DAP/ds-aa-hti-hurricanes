"""Data behind the advisory-by-advisory map (``docs/carte-avis.html``).

An index file - department polygons, thresholds, the storm list - and one
file per storm with every NHC advisory: forecast track, wind buffers at
34 / 50 / 64 kt (built with the same ocha-lens helpers the exposure
trigger uses), the observed swath accumulated by the issuance, the
per-department gust and CHIRPS-GEFS rain readings, and the framework's
pathway values. The page's JavaScript applies the thresholds, which are
written once, in the index ``meta``.
"""

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from ocha_lens.utils.storm import calculate_wind_buffers_gdf, expand_quad_col
from shapely.geometry import mapping
from sqlalchemy import text

from src.constants import EXPOSURE_WIND_KT, LT_CUTOFF_HRS, TRIGGERS
from src.datasources import codab
from src.datasources import storms_db as sdb
from src.dgpc import constants as dc
from src.dgpc import pathways as pw
from src.dgpc.dept_page import DEPT_ORDER, PROPOSED_N
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

WIND_LEVELS = (34, 50, 64)
BUFFER_SIMPLIFY_DEG = 0.01

TRACK_Q = """
SELECT atcf_id, basin, issued_time, valid_time, leadtime, wind_speed,
       quadrant_radius_34, quadrant_radius_50, quadrant_radius_64, geometry
FROM storms.nhc_tracks_geo
WHERE provider = 'nhc' AND leadtime IS NOT NULL AND atcf_id = :aid
ORDER BY issued_time, leadtime
"""
OBSV_BUF_Q = """
SELECT valid_time, geometry
FROM storms.nhc_tracks_obsv_buffers
WHERE atcf_id = :aid AND wind_speed_kt = :kt
ORDER BY valid_time
"""


def _r(v, nd=1):
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return None
    return round(float(v), nd)


def _geom(g):
    """Compact GeoJSON geometry, or None."""
    if g is None or g.is_empty:
        return None
    g = g.simplify(BUFFER_SIMPLIFY_DEG, preserve_topology=True)
    if g.is_empty:
        return None
    return json.loads(
        json.dumps(mapping(g)), parse_float=lambda x: round(float(x), 3)
    )


def _depts_geojson():
    adm1 = codab.load_codab_from_blob(admin_level=1)[["ADM1_FR", "geometry"]]
    adm1 = adm1.rename(columns={"ADM1_FR": "name"})
    adm1["geometry"] = adm1.geometry.simplify(0.004, preserve_topology=True)
    return json.loads(adm1.to_json(drop_id=True))


def _buffers(tp: gpd.GeoDataFrame):
    """Wind buffers for one advisory, keyed by level, as GeoJSON."""
    has = tp[[f"quadrant_radius_{s}" for s in WIND_LEVELS]].notna().any(axis=1)
    tp = tp[has]
    if tp.empty:
        return {}
    for s in WIND_LEVELS:
        tp = expand_quad_col(tp, f"quadrant_radius_{s}")
    try:
        buf = calculate_wind_buffers_gdf(
            tp, quad_cols_format="quadrant_radius_{speed}_{quad}"
        ).set_index("wind_speed_kt")
    except Exception as exc:  # noqa: BLE001
        logger.warning("buffer failed: %s", exc)
        return {}
    out = {}
    for s in WIND_LEVELS:
        if s in buf.index:
            g = _geom(buf.loc[s, "geometry"])
            if g:
                out[str(s)] = g
    return out


def _storm_file(s, adv, fc, rain_long, wcols, variant, engine):
    aid = s.atcf_id
    with engine.connect() as conn:
        tracks = gpd.read_postgis(
            text(TRACK_Q), conn, params={"aid": aid}, geom_col="geometry"
        )
        obsv_buf = gpd.read_postgis(
            text(OBSV_BUF_Q),
            conn,
            params={"aid": aid, "kt": EXPOSURE_WIND_KT},
            geom_col="geometry",
        )
    obsv = (
        tracks[tracks.leadtime == 0]
        .sort_values("issued_time")
        .geometry.apply(lambda p: [round(p.y, 2), round(p.x, 2)])
        .tolist()
    )

    advs = []
    for _, a in adv[adv.atcf_id == aid].sort_values("issued_time").iterrows():
        tp = tracks[tracks.issued_time == a.issued_time].sort_values(
            "leadtime"
        )
        track = [
            [round(p.y, 2), round(p.x, 2), int(lt), _r(ws, 0)]
            for p, lt, ws in zip(tp.geometry, tp.leadtime, tp.wind_speed)
        ]
        key = (aid, a.issued_time)
        if key in fc.index:
            row = fc.loc[key]
            w = [_r(row.get(wcols[d], np.nan) * 1.852, 0) for d in DEPT_ORDER]
        else:
            w = [None] * len(DEPT_ORDER)
        rp = rm = [None] * len(DEPT_ORDER)
        if pd.notna(a.gefs_issue_date):
            rs = rain_long[
                (rain_long.atcf_id == aid)
                & (rain_long.issue_date == a.gefs_issue_date)
                & (rain_long.valid_date >= a.rain_window_start)
                & (rain_long.valid_date <= a.rain_window_end)
            ]
            if len(rs):
                g = rs.groupby("dept")
                pmax, mmax = g["pix_max_mm"].max(), g["mean_mm"].max()
                rp = [_r(pmax.get(d, np.nan), 0) for d in DEPT_ORDER]
                rm = [_r(mmax.get(d, np.nan), 0) for d in DEPT_ORDER]
        # observed 64 kt swath accumulated by this issuance
        prior = obsv_buf[obsv_buf.valid_time <= a.issued_time]
        swath = _geom(prior.iloc[-1].geometry) if len(prior) else None
        advs.append(
            {
                "t": pd.Timestamp(a.issued_time).strftime("%Y-%m-%dT%H:%M"),
                "ttc": _r(a.time_to_closest_h, 0),
                "cut": bool(a.past_cutoff),
                "rain": _r(a.fcast_rain_mm, 0),
                "exp": int(a.fcast_exp_64) if pd.notna(a.fcast_exp_64) else 0,
                "gefs": (
                    pd.Timestamp(a.gefs_issue_date).strftime("%Y-%m-%d")
                    if pd.notna(a.gefs_issue_date)
                    else None
                ),
                "track": track,
                "buf": _buffers(tp),
                "swath": swath,
                "w": w,
                "rp": rp,
                "rm": rm,
            }
        )
    name = str(s["name"]).title()
    return {
        "id": aid,
        "label": f"{name} {int(s.season)}",
        "closest": pd.Timestamp(s.closest_time).strftime("%Y-%m-%dT%H:%M"),
        "obsv": obsv,
        "adv": advs,
    }


def write(out_dir, only=None):
    """Write ``index.json`` and one ``<atcf_id>.json`` per storm."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    P = pw.PREFIX
    storms = blob.load_parquet_from_blob(f"{P}/storm_set.parquet")
    adv = blob.load_parquet_from_blob(f"{P}/pathways_advisories.parquet")
    fcast = blob.load_parquet_from_blob(f"{P}/fcast_wind_by_issuance.parquet")
    rain_long = blob.load_parquet_from_blob(f"{P}/dept_rain_fcast.parquet")
    exp_adv, _ = pw.exposure_by_advisory(adv)
    adv = adv.merge(exp_adv, on=["atcf_id", "issued_time"], how="left")

    variant = dc.ORANGE_WIND_VARIANT
    wcols = {d: f"{variant}_dept_{d}_kt" for d in DEPT_ORDER}
    fc = fcast.set_index(["atcf_id", "issued_time"])

    storms = storms.sort_values("first_near")
    index = {
        "meta": {
            "cutoff_h": LT_CUTOFF_HRS,
            "wind_kmh": dc.ORANGE_WIND_KMH[0],
            "rain_mm": dc.ORANGE_RAIN["threshold_mm"],
            "fcast_rain_mm": TRIGGERS["mobilisation"]["rain_mm"],
            "exposure_kt": EXPOSURE_WIND_KT,
            "n_depts": PROPOSED_N,
            "depts": DEPT_ORDER,
            "levels": list(WIND_LEVELS),
        },
        "depts": _depts_geojson(),
        "storms": [
            {
                "id": s.atcf_id,
                "label": f"{str(s['name']).title()} {int(s.season)}",
                "season": int(s.season),
            }
            for _, s in storms.iterrows()
        ],
    }
    with open(out_dir / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, separators=(",", ":"))

    # One engine for the whole run: a fresh pool per storm leaks
    # connections until the server refuses new ones.
    engine = sdb.get_engine()
    for _, s in storms.iterrows():
        if only and s.atcf_id not in only:
            continue
        data = _storm_file(s, adv, fc, rain_long, wcols, variant, engine)
        path = out_dir / f"{s.atcf_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        logger.info(
            "%s: %d advisories, %.0f kB",
            s.atcf_id,
            len(data["adv"]),
            path.stat().st_size / 1024,
        )
    engine.dispose()
    return index
