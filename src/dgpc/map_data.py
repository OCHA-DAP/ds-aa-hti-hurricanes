"""Data behind the advisory-by-advisory map on the department page.

One JSON file: the department polygons (simplified), and for every storm
every NHC advisory with its forecast track, its per-department gust and
CHIRPS-GEFS rain readings, and the framework's pathway values at that
advisory. The page's JavaScript does the rest (cumulative orange, the
thresholds, the toggles), so the thresholds are written once, in
``meta``.
"""

import json

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.constants import LT_CUTOFF_HRS, TRIGGERS
from src.datasources import codab
from src.datasources import storms_db as sdb
from src.dgpc import constants as dc
from src.dgpc import pathways as pw
from src.dgpc.dept_page import DEPT_ORDER, PROPOSED_N
from src.utils import blob

TRACK_XY_Q = """
SELECT atcf_id, issued_time, leadtime, wind_speed,
       ST_Y(geometry) lat, ST_X(geometry) lon
FROM storms.nhc_tracks_geo
WHERE provider = 'nhc' AND leadtime IS NOT NULL AND atcf_id IN :ids
ORDER BY atcf_id, issued_time, leadtime
"""


def _r(v, nd=1):
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return None
    return round(float(v), nd)


def _depts_geojson():
    adm1 = codab.load_codab_from_blob(admin_level=1)[["ADM1_FR", "geometry"]]
    adm1 = adm1.rename(columns={"ADM1_FR": "name"})
    adm1["geometry"] = adm1.geometry.simplify(0.004, preserve_topology=True)
    return json.loads(adm1.to_json(drop_id=True))


def build():
    P = pw.PREFIX
    storms = blob.load_parquet_from_blob(f"{P}/storm_set.parquet")
    adv = blob.load_parquet_from_blob(f"{P}/pathways_advisories.parquet")
    fcast = blob.load_parquet_from_blob(f"{P}/fcast_wind_by_issuance.parquet")
    rain_long = blob.load_parquet_from_blob(f"{P}/dept_rain_fcast.parquet")
    exp_adv, _ = pw.exposure_by_advisory(adv)
    adv = adv.merge(exp_adv, on=["atcf_id", "issued_time"], how="left")

    ids = tuple(storms.atcf_id)
    with sdb.get_engine().connect() as conn:
        pts = pd.read_sql(text(TRACK_XY_Q), conn, params={"ids": ids})

    variant = dc.ORANGE_WIND_VARIANT
    wcols = {d: f"{variant}_dept_{d}_kt" for d in DEPT_ORDER}
    fc = fcast.set_index(["atcf_id", "issued_time"])

    out_storms = []
    for _, s in storms.sort_values("first_near").iterrows():
        aid = s.atcf_id
        sp = pts[pts.atcf_id == aid]
        obsv = (
            sp[sp.leadtime == 0]
            .sort_values("issued_time")[["lat", "lon"]]
            .round(2)
            .values.tolist()
        )
        advs = []
        for _, a in (
            adv[adv.atcf_id == aid].sort_values("issued_time").iterrows()
        ):
            tp = sp[sp.issued_time == a.issued_time].sort_values("leadtime")
            track = [
                [_r(la, 2), _r(lo, 2), int(lt)]
                for la, lo, lt in zip(tp.lat, tp.lon, tp.leadtime)
            ]
            key = (aid, a.issued_time)
            if key in fc.index:
                row = fc.loc[key]
                w = [
                    _r(row.get(wcols[d], np.nan) * 1.852, 0)
                    for d in DEPT_ORDER
                ]
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
            advs.append(
                {
                    "t": pd.Timestamp(a.issued_time).strftime(
                        "%Y-%m-%dT%H:%M"
                    ),
                    "ttc": _r(a.time_to_closest_h, 0),
                    "cut": bool(a.past_cutoff),
                    "rain": _r(a.fcast_rain_mm, 0),
                    "exp": (
                        int(a.fcast_exp_64) if pd.notna(a.fcast_exp_64) else 0
                    ),
                    "gefs": (
                        pd.Timestamp(a.gefs_issue_date).strftime("%Y-%m-%d")
                        if pd.notna(a.gefs_issue_date)
                        else None
                    ),
                    "track": track,
                    "w": w,
                    "rp": rp,
                    "rm": rm,
                }
            )
        name = str(s["name"]).title()
        out_storms.append(
            {
                "id": aid,
                "label": f"{name} {int(s.season)}",
                "closest": pd.Timestamp(s.closest_time).strftime(
                    "%Y-%m-%dT%H:%M"
                ),
                "obsv": obsv,
                "adv": advs,
            }
        )

    return {
        "meta": {
            "cutoff_h": LT_CUTOFF_HRS,
            "wind_kmh": dc.ORANGE_WIND_KMH[0],
            "rain_mm": dc.ORANGE_RAIN["threshold_mm"],
            "fcast_rain_mm": TRIGGERS["mobilisation"]["rain_mm"],
            "n_depts": PROPOSED_N,
            "depts": DEPT_ORDER,
        },
        "depts": _depts_geojson(),
        "storms": out_storms,
    }


def write(path):
    data = build()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    return data
