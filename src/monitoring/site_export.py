"""Data behind the public monitoring page (``/suivi/`` on the Pages site).

Runs after every monitoring run (task ``export_site`` of the DBX job, see
databricks.yml). It republishes what the monitoring recorded, together
with the inputs behind each number, as static files that the page reads
and that anyone can download:

    index.json                      storms, activations, thresholds, data
                                    freshness, latest CHIRPS-GEFS / IMERG
    storms/{ATCF}.json              one storm: every advisory row, the
                                    observed track and swath, the observed
                                    rainfall days and window
    advisories/{ATCF}/{stamp}.geojson
                                    one NHC advisory: forecast track, the
                                    64 kt buffers capped at 120 h and 72 h,
                                    the observed swath, the CHIRPS-GEFS
                                    issuance and each stage's rain window
    fcast_monitoring.csv            the monitoring tables as recorded
    obsv_monitoring.csv
    rain/chirps_gefs.csv            rain series since MONITORING_START
    rain/imerg.csv
    manifest.json                   sha256 + size of every file, written last

Files live in dev blob under ``SITE_PREFIX``; the Pages workflow downloads
them and checks them against the manifest (pipelines/sync_site_data.py).
Advisory files are written once; everything else is rewritten each run.

Trigger values are always the ones the monitoring RECORDED (the v2
parquets), never recomputed ones. The geometries and rain windows are
rebuilt by the function the monitoring itself uses
(``process_fcast_advisory(detail=True)``, pinned to the recorded
CHIRPS-GEFS issuance); the rebuilt values are compared with the recorded
ones and any difference is written into the advisory file.
"""

import hashlib
import io
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import mapping
from sqlalchemy import text

from src.constants import (
    D_THRESH,
    EXPOSURE_WIND_KT,
    LT_CUTOFF_HRS,
    MIN_EMAIL_DISTANCE,
    OVERALL_RP_YEARS,
    STAGE_NAMES_FR,
    TRIGGERS,
)
from src.datasources import codab, imerg, storms_db
from src.monitoring import monitoring_utils as mu
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

SITE_PREFIX = f"{blob.PROJECT_PREFIX}/monitoring/site"
SCHEMA_VERSION = 1

# Display simplification of exported polygons (degrees, ~100 m) and
# coordinate rounding (4 decimals, ~11 m). Exposure values are computed on
# the full-resolution geometry before this.
SIMPLIFY_DEG = 0.001
COORD_DECIMALS = 4

# Recomputed vs recorded: differences under these are rounding noise.
TOL = {"rain": 0.01, "pop": 0.5}

# Storm considered "active" on the page while its last advisory is at most
# this old when the export runs.
ACTIVE_HRS = 18


# --- serialisation helpers ------------------------------------------------


def _iso(ts) -> str | None:
    if ts is None or (not isinstance(ts, str) and pd.isna(ts)):
        return None
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _date(d) -> str | None:
    if d is None or (not isinstance(d, str) and pd.isna(d)):
        return None
    return pd.Timestamp(d).strftime("%Y-%m-%d")


def _num(x, nd: int | None = None):
    """JSON-safe number: None for NaN/None, rounded if nd is given."""
    if x is None:
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return round(x, nd) if nd is not None else x


def _bool(x) -> bool | None:
    if x is None or (not isinstance(x, (bool, np.bool_)) and pd.isna(x)):
        return None
    return bool(x)


def _hours(td) -> float | None:
    if td is None or pd.isna(td):
        return None
    return round(pd.Timedelta(td).total_seconds() / 3600, 2)


def stamp(issue_time) -> str:
    """File-name-safe advisory stamp, e.g. 2026-10-22T0900Z."""
    return pd.Timestamp(issue_time).strftime("%Y-%m-%dT%H%MZ")


def _round_coords(obj):
    if isinstance(obj, (list, tuple)):
        if obj and isinstance(obj[0], (int, float)):
            return [round(float(v), COORD_DECIMALS) for v in obj]
        return [_round_coords(o) for o in obj]
    return obj


def _geojson_geom(geom):
    """Simplified, rounded GeoJSON geometry dict (None for empty)."""
    if geom is None or geom.is_empty:
        return None
    g = shapely.simplify(geom, SIMPLIFY_DEG, preserve_topology=True)
    if g.is_empty:
        g = geom
    m = mapping(g)
    return {"type": m["type"], "coordinates": _round_coords(m["coordinates"])}


def _dumps(obj) -> bytes:
    return json.dumps(
        obj, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


# --- storage --------------------------------------------------------------


class SiteStore:
    """Where the site files go: dev blob (default) or a local directory."""

    def __init__(self, local_dir: str | None = None):
        self.local = Path(local_dir) if local_dir else None
        self.manifest = self._load_manifest()

    def _load_manifest(self) -> dict:
        try:
            raw = self.read("manifest.json")
        except Exception:
            return {}
        return json.loads(raw).get("files", {})

    def read(self, path: str) -> bytes:
        if self.local:
            return (self.local / path).read_bytes()
        return blob.load_blob_data(f"{SITE_PREFIX}/{path}")

    def existing(self, prefix: str) -> set[str]:
        if self.local:
            root = self.local / prefix
            if not root.exists():
                return set()
            return {
                str(p.relative_to(self.local))
                for p in root.rglob("*")
                if p.is_file()
            }
        names = blob.list_container_blobs(
            name_starts_with=f"{SITE_PREFIX}/{prefix}"
        )
        return {n.removeprefix(f"{SITE_PREFIX}/") for n in names}

    def write(self, path: str, data: bytes, content_type: str):
        self.manifest[path] = {
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
        }
        if self.local:
            out = self.local / path
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(data)
            return
        blob.upload_blob_data(
            f"{SITE_PREFIX}/{path}", data, content_type=content_type
        )

    def write_json(self, path: str, obj):
        ct = (
            "application/geo+json"
            if path.endswith(".geojson")
            else "application/json"
        )
        self.write(path, _dumps(obj), ct)

    def write_csv(self, path: str, df: pd.DataFrame):
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        self.write(path, buf.getvalue().encode("utf-8"), "text/csv")

    def finish(self, generated_at: str):
        """Write the manifest last: a deploy never sees a half-run."""
        self.write_json(
            "manifest.json",
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": generated_at,
                "files": dict(sorted(self.manifest.items())),
            },
        )


# --- inputs ---------------------------------------------------------------


OBSV_COLS = [
    "monitor_id",
    "atcf_id",
    "name",
    "issue_time",
    "min_dist",
    "obsv_rain",
    "obsv_exp",
    "rainfall_relevant",
    "rain_trigger_obsv",
    "exp_trigger_obsv",
    "obsv_trigger",
]


def _load_recorded():
    fc = mu.load_existing_monitoring_points("fcast")
    ob = mu.load_existing_monitoring_points("obsv")
    if "atcf_id" not in ob:  # no observation parquet yet (new season/setup)
        ob = pd.DataFrame(columns=OBSV_COLS)
    for df in (fc, ob):
        if "issue_time" in df:
            df["issue_time"] = pd.to_datetime(df["issue_time"])
    return fc, ob


def _gefs_series(df_gefs_all: pd.DataFrame, issue_date) -> pd.DataFrame:
    """One CHIRPS-GEFS issuance with the monitoring's 2-day sum."""
    g = df_gefs_all[df_gefs_all["issue_date"] == issue_date].copy()
    g["roll2_sum"] = (
        g["mean"].rolling(window=2, center=True, min_periods=1).sum()
    )
    return g


def _load_imerg() -> pd.DataFrame:
    im = imerg.load_imerg_from_postgres(recent=True)
    im = im.sort_values("date").reset_index(drop=True)
    im["roll2_sum"] = (
        im["mean"].rolling(window=2, center=True, min_periods=1).sum()
    )
    return im


def _fetch_obsv_buffer_latest(atcf_id: str, at_time=None):
    """Latest cumulative observed 64 kt swath at/before at_time."""
    params = {"atcf_id": atcf_id.upper(), "kt": EXPOSURE_WIND_KT}
    where = "atcf_id = :atcf_id AND wind_speed_kt = :kt"
    if at_time is not None:
        where += " AND valid_time <= :at_time"
        params["at_time"] = storms_db.naive_utc(at_time)
    query = text(
        f"""
        SELECT valid_time, geometry FROM storms.nhc_tracks_obsv_buffers
        WHERE {where} ORDER BY valid_time DESC LIMIT 1
        """
    )
    with storms_db.get_engine().connect() as conn:
        gdf = gpd.read_postgis(query, conn, params=params, geom_col="geometry")
    if gdf.empty:
        return None, None
    return gdf.iloc[0]["valid_time"], gdf.iloc[0]["geometry"]


# --- advisory files -------------------------------------------------------


def _stage_block(rec: pd.Series, stage: str, window, rebuilt: bool) -> dict:
    lt = TRIGGERS[stage]["lt_max_hrs"]
    rain = _num(rec.get(f"rain_{lt}h"), 2)
    if rebuilt:
        eligible = window is not None
    else:
        # no window to show: a recorded value means there was one, a
        # missing value is undecidable (no window, or no CHIRPS-GEFS days)
        eligible = True if rain is not None else None
    return {
        "name_fr": STAGE_NAMES_FR[stage],
        "lt_max_hrs": lt,
        "rain_threshold_mm": TRIGGERS[stage]["rain_mm"],
        "rain_window_start": _date(window[0]) if window else None,
        "rain_window_end": _date(window[1]) if window else None,
        "rain_eligible": eligible,
        "rain_2day_max_mm": rain,
        "rain_met": _bool(rec.get(f"rain_trigger_{stage}")),
        "exp_fcast": _num(rec.get(f"exp_fcast_{lt}h"), 0),
        "exp_total": _num(rec.get(f"exp_total_{lt}h"), 0),
        "exp_met": _bool(rec.get(f"exp_trigger_{stage}")),
        "triggered": _bool(rec.get(f"{stage}_trigger")),
    }


def _compare(rec: pd.Series, row: dict) -> dict:
    """Recorded vs rebuilt values that differ beyond rounding."""
    diffs = {}
    keys = [
        ("rain_120h", "rain"),
        ("rain_72h", "rain"),
        ("exp_fcast_120h", "pop"),
        ("exp_fcast_72h", "pop"),
        ("exp_obsv", "pop"),
        ("past_cutoff", None),
        ("mobilisation_trigger", None),
        ("action_trigger", None),
    ]
    for k, kind in keys:
        a, b = rec.get(k), row.get(k)
        if kind is None:
            same = _bool(a) == _bool(b)
        else:
            a, b = _num(a), _num(b)
            same = (a is None and b is None) or (
                a is not None and b is not None and abs(a - b) <= TOL[kind]
            )
        if not same:
            diffs[k] = {"recorded": _jsonable(a), "rebuilt": _jsonable(b)}
    return diffs


def _jsonable(x):
    if isinstance(x, (bool, np.bool_)):
        return bool(x)
    return _num(x) if x is not None else None


def build_advisory(
    rec: pd.Series, df_gefs_all: pd.DataFrame, adm0_3857
) -> dict:
    """GeoJSON FeatureCollection for one recorded advisory row."""
    atcf_id, issue_time = rec["atcf_id"], pd.Timestamp(rec["issue_time"])
    gefs_date = rec.get("gefs_issue_date")
    gefs_date = None if gefs_date is None or pd.isna(gefs_date) else gefs_date
    res = mu.process_fcast_advisory(
        atcf_id,
        issue_time,
        df_gefs_all,
        adm0_3857,
        name=rec["name"],
        detail=True,
        gefs_issue_date=gefs_date,
    )
    features = []
    windows = {"mobilisation": None, "action": None}
    gefs_block = None
    rebuilt = {"available": False, "differences": {}}
    closest_time = None
    if res is not None:
        row, det = res
        windows = det["windows"]
        closest_time = det["closest_time"]
        rebuilt = {"available": True, "differences": _compare(rec, row)}
        if rebuilt["differences"]:
            logger.warning(
                f"{rec['monitor_id']}: rebuilt values differ from the "
                f"record: {rebuilt['differences']}"
            )
        # forecast track points
        trk = det["track"].copy()
        trk_3857 = trk.to_crs(3857)
        trk["dist_km"] = (
            trk_3857.geometry.distance(adm0_3857.iloc[0].geometry) / 1000
        )
        for _, p in trk.iterrows():
            lt = _num(p["leadtime"])
            features.append(
                {
                    "type": "Feature",
                    "geometry": _geojson_geom(p.geometry),
                    "properties": {
                        "layer": "fcast_track",
                        "valid_time": _iso(p["valid_time"]),
                        "leadtime_hrs": lt,
                        "wind_kt": _num(p["wind_speed"]),
                        "dist_km": _num(p["dist_km"], 1),
                        "within_120h": lt is not None and lt <= 120,
                        "within_72h": lt is not None and lt <= 72,
                    },
                }
            )
        # capped forecast buffers (forecast-only: observed swath removed)
        for stage in mu.FCAST_STAGES:
            lt = TRIGGERS[stage]["lt_max_hrs"]
            buf = det["buffers"][stage]
            g = _geojson_geom(buf["geometry"])
            if g is None:
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": g,
                    "properties": {
                        "layer": f"fcast_{EXPOSURE_WIND_KT}kt_le{lt}h",
                        "stage": stage,
                        "lt_max_hrs": lt,
                        "wind_kt": EXPOSURE_WIND_KT,
                        "pop_exposed_hti": _num(
                            rec.get(f"exp_fcast_{lt}h"), 0
                        ),
                    },
                }
            )
        # gefs series actually used
        g = det["gefs"]
        gefs_block = {
            "issue_date": _date(det["gefs_issue_date"]),
            "days": [
                {
                    "date": _date(r["valid_date"]),
                    "mean_mm": _num(r["mean"], 2),
                    "sum_2day_mm": _num(r["roll2_sum"], 2),
                    **{
                        f"in_window_{s}": bool(
                            windows[s] is not None
                            and windows[s][0]
                            <= pd.Timestamp(r["valid_date"]).date()
                            <= windows[s][1]
                        )
                        for s in mu.FCAST_STAGES
                    },
                }
                for _, r in g.sort_values("valid_date").iterrows()
            ],
        }
    else:
        logger.warning(
            f"{rec['monitor_id']}: advisory no longer rebuildable from the "
            "storms DB; publishing the recorded values without geometry."
        )

    # observed swath at issuance (what exp_obsv counts). The forecast
    # buffers only had a swath removed when one existed at exactly the
    # issue time (fetch_obsv_buffer_at, as in ds-storms-pipeline); flag it.
    sw_time, sw_geom = _fetch_obsv_buffer_latest(atcf_id, issue_time)
    subtracted = None
    if res is not None:
        subtracted = any(
            det["buffers"][s]["obsv_geometry"] is not None
            for s in mu.FCAST_STAGES
        )
    g = _geojson_geom(sw_geom)
    if g is not None:
        features.append(
            {
                "type": "Feature",
                "geometry": g,
                "properties": {
                    "layer": f"obsv_{EXPOSURE_WIND_KT}kt_swath",
                    "swath_valid_time": _iso(sw_time),
                    "subtracted_from_forecast": subtracted,
                    "wind_kt": EXPOSURE_WIND_KT,
                    "pop_exposed_hti": _num(rec.get("exp_obsv"), 0),
                },
            }
        )

    hrs = _hours(rec.get("time_to_closest"))
    advisory = {
        "schema_version": SCHEMA_VERSION,
        "monitor_id": rec["monitor_id"],
        "atcf_id": atcf_id.upper(),
        "name": rec["name"],
        "issue_time": _iso(issue_time),
        "closest_pass_time": _iso(closest_time),
        "hours_to_closest_pass": hrs,
        "cutoff_hrs": LT_CUTOFF_HRS,
        "past_cutoff": _bool(rec.get("past_cutoff")),
        "min_dist_km": _num(rec.get("min_dist"), 1),
        "closest_wind_kt": _num(rec.get("closest_s")),
        "exp_obsv": _num(rec.get("exp_obsv"), 0),
        "stages": {
            s: _stage_block(rec, s, windows[s], rebuilt["available"])
            for s in mu.FCAST_STAGES
        },
        "chirps_gefs": gefs_block,
        "rebuilt": rebuilt,
    }
    return {
        "type": "FeatureCollection",
        "advisory": advisory,
        "features": features,
    }


# --- storm files ----------------------------------------------------------


def _activations(fc: pd.DataFrame, ob: pd.DataFrame) -> dict:
    """First advisory / observation at which each stage fired.

    Réponse précoce: the emails send no separate activation for a storm
    whose Action already fired (src/email/update_emails.py), so a storm
    without Action is preferred and ``separate_activation`` says which.
    """
    out = {}
    action_storms = (
        set(fc.loc[fc["action_trigger"].fillna(False).astype(bool), "atcf_id"])
        if "action_trigger" in fc
        else set()
    )
    if "obsv_trigger" in ob and not ob.empty:
        ob = ob.assign(_sep=~ob["atcf_id"].isin(action_storms)).sort_values(
            ["_sep", "issue_time"], ascending=[False, True]
        )
    for stage, df in (
        ("mobilisation", fc),
        ("action", fc),
        ("obsv", ob),
    ):
        col = f"{stage}_trigger"
        hit = (
            df[df[col].fillna(False).astype(bool)]
            if col in df and not df.empty
            else df.iloc[0:0]
        )
        if stage != "obsv":
            hit = hit.sort_values("issue_time")
        if hit.empty:
            out[stage] = None
            continue
        out[stage] = {
            "atcf_id": hit.iloc[0]["atcf_id"].upper(),
            "monitor_id": hit.iloc[0]["monitor_id"],
            "issue_time": _iso(hit.iloc[0]["issue_time"]),
        }
        if stage == "obsv":
            out[stage]["separate_activation"] = bool(hit.iloc[0]["_sep"])
    return out


def _fcast_summary_row(r: pd.Series) -> dict:
    d = {
        "monitor_id": r["monitor_id"],
        "issue_time": _iso(r["issue_time"]),
        "file": f"advisories/{r['atcf_id'].upper()}/"
        f"{stamp(r['issue_time'])}.geojson",
        "hours_to_closest_pass": _hours(r.get("time_to_closest")),
        "past_cutoff": _bool(r.get("past_cutoff")),
        "min_dist_km": _num(r.get("min_dist"), 1),
        "exp_obsv": _num(r.get("exp_obsv"), 0),
    }
    for stage in mu.FCAST_STAGES:
        lt = TRIGGERS[stage]["lt_max_hrs"]
        d[f"rain_{lt}h"] = _num(r.get(f"rain_{lt}h"), 2)
        d[f"exp_fcast_{lt}h"] = _num(r.get(f"exp_fcast_{lt}h"), 0)
        d[f"exp_total_{lt}h"] = _num(r.get(f"exp_total_{lt}h"), 0)
        d[f"rain_trigger_{stage}"] = _bool(r.get(f"rain_trigger_{stage}"))
        d[f"exp_trigger_{stage}"] = _bool(r.get(f"exp_trigger_{stage}"))
        d[f"{stage}_trigger"] = _bool(r.get(f"{stage}_trigger"))
    return d


def build_storm(
    atcf_id: str,
    fc: pd.DataFrame,
    ob: pd.DataFrame,
    im: pd.DataFrame,
    adm0_3857,
) -> dict:
    fc = fc.sort_values("issue_time")
    ob = ob.sort_values("issue_time")
    name = fc["name"].iloc[-1] if not fc.empty else ob["name"].iloc[-1]

    # observed track and its 230 km rainfall window (as update_obsv_monitoring)
    trk = storms_db.fetch_obsv_track(atcf_id)
    track_feats, window = [], None
    if not trk.empty:
        t3857 = trk.to_crs(3857)
        trk["dist_km"] = (
            t3857.geometry.distance(adm0_3857.iloc[0].geometry) / 1000
        )
        for _, p in trk.iterrows():
            track_feats.append(
                {
                    "type": "Feature",
                    "geometry": _geojson_geom(p.geometry),
                    "properties": {
                        "valid_time": _iso(p["valid_time"]),
                        "wind_kt": _num(p["wind_speed"]),
                        "dist_km": _num(p["dist_km"], 1),
                    },
                }
            )
        gi = mu._interp_track(trk, "valid_time", adm0_3857)
        near = gi[gi["distance"] < D_THRESH]
        if not near.empty:
            window = (
                near["valid_time"].min().date(),
                (near["valid_time"].max() + pd.Timedelta(days=1)).date(),
            )

    # IMERG days around the storm's life, flagged with the window
    days = []
    if not trk.empty:
        lo = trk["valid_time"].min().normalize() - pd.Timedelta(days=2)
        hi = trk["valid_time"].max().normalize() + pd.Timedelta(days=3)
        for _, r in im[(im["date"] >= lo) & (im["date"] <= hi)].iterrows():
            dd = r["date"].date()
            days.append(
                {
                    "date": _date(r["date"]),
                    "mean_mm": _num(r["mean"], 2),
                    "sum_2day_mm": _num(r["roll2_sum"], 2),
                    "in_window": bool(
                        window is not None and window[0] <= dd <= window[1]
                    ),
                }
            )

    sw_time, sw_geom = _fetch_obsv_buffer_latest(atcf_id)
    obsv_rows = [
        {
            "monitor_id": r["monitor_id"],
            "issue_time": _iso(r["issue_time"]),
            "imerg_through": _date(
                r["issue_time"] - pd.Timedelta(days=1, hours=15)
            ),
            "min_dist_km": _num(r.get("min_dist"), 1),
            "obsv_rain": _num(r.get("obsv_rain"), 2),
            "obsv_exp": _num(r.get("obsv_exp"), 0),
            "rainfall_relevant": _bool(r.get("rainfall_relevant")),
            "rain_trigger_obsv": _bool(r.get("rain_trigger_obsv")),
            "exp_trigger_obsv": _bool(r.get("exp_trigger_obsv")),
            "obsv_trigger": _bool(r.get("obsv_trigger")),
        }
        for _, r in ob.iterrows()
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "atcf_id": atcf_id.upper(),
        "name": name,
        "activations": _activations(fc, ob),
        "advisories": [_fcast_summary_row(r) for _, r in fc.iterrows()],
        "obsv": {
            "rain_threshold_mm": TRIGGERS["obsv"]["rain_mm"],
            "rain_window_start": _date(window[0]) if window else None,
            "rain_window_end": _date(window[1]) if window else None,
            "rows": obsv_rows,
            "imerg_days": days,
        },
        "obsv_track": {"type": "FeatureCollection", "features": track_feats},
        "obsv_swath": {
            "valid_time": _iso(sw_time),
            "wind_kt": EXPOSURE_WIND_KT,
            "geometry": _geojson_geom(sw_geom),
        },
    }


# --- tables ---------------------------------------------------------------


def _fcast_table(fc: pd.DataFrame) -> pd.DataFrame:
    out = fc.copy()
    out.insert(
        out.columns.get_loc("time_to_closest") + 1,
        "hours_to_closest_pass",
        out["time_to_closest"].map(_hours),
    )
    out = out.drop(columns=["time_to_closest"])
    out["issue_time"] = out["issue_time"].map(_iso)
    if "gefs_issue_date" in out:
        out["gefs_issue_date"] = out["gefs_issue_date"].map(_date)
    out["file"] = [
        f"advisories/{a.upper()}/{stamp(t)}.geojson"
        for a, t in zip(fc["atcf_id"], fc["issue_time"])
    ]
    return out


def _geo_layers(adm0_3857) -> dict:
    """Haiti outline and the distance zones the monitoring uses.

    Distances in the monitoring are taken in EPSG:3857 metres from the
    CODAB outline, so the zones are buffered the same way (at Haiti's
    latitude, 230 km in 3857 is ~217 km on the ground).
    """
    feats = []
    outline = adm0_3857.geometry.union_all()
    for layer, dist_km, tol in (
        ("hti_adm0", 0, 0.005),
        ("rain_zone", D_THRESH, 0.02),
        ("info_zone", MIN_EMAIL_DISTANCE, 0.05),
    ):
        g = outline.buffer(dist_km * 1000) if dist_km else outline
        g = gpd.GeoSeries([g], crs=3857).to_crs(4326).iloc[0]
        g = shapely.simplify(g, tol, preserve_topology=True)
        m = mapping(g)
        feats.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": m["type"],
                    "coordinates": _round_coords(m["coordinates"]),
                },
                "properties": {"layer": layer, "dist_km": dist_km},
            }
        )
    return {"type": "FeatureCollection", "features": feats}


# --- entry point ----------------------------------------------------------


def export_site(local_dir: str | None = None, full: bool = False):
    """Write the site data. ``full`` rewrites every advisory file."""
    generated_at = _iso(datetime.now(timezone.utc))
    store = SiteStore(local_dir)
    adm0 = codab.load_codab_from_blob().to_crs(3857)
    fc, ob = _load_recorded()
    if fc.empty:
        logger.info("No forecast monitoring rows yet; nothing to export.")
        return
    df_gefs_all = mu.load_gefs_with_issue_times()
    im = _load_imerg()

    # advisory files (immutable; only the missing ones)
    have = set() if full else store.existing("advisories/")
    n_new = 0
    for _, rec in fc.sort_values("issue_time").iterrows():
        path = (
            f"advisories/{rec['atcf_id'].upper()}/"
            f"{stamp(rec['issue_time'])}.geojson"
        )
        if path in have and path in store.manifest:
            continue
        store.write_json(path, build_advisory(rec, df_gefs_all, adm0))
        n_new += 1
    logger.info(f"Advisory files written: {n_new}")

    # storm files (rewritten each run)
    storms = []
    latest_issue = fc["issue_time"].max()
    now = pd.Timestamp(generated_at).tz_localize(None)
    season = int(latest_issue.year)
    for atcf_id in sorted(set(fc["atcf_id"]) | set(ob["atcf_id"])):
        f_s = fc[fc["atcf_id"] == atcf_id]
        o_s = ob[ob["atcf_id"] == atcf_id]
        storm = build_storm(atcf_id, f_s, o_s, im, adm0)
        store.write_json(f"storms/{atcf_id.upper()}.json", storm)
        last = f_s["issue_time"].max() if not f_s.empty else None
        latest_row = (
            _fcast_summary_row(f_s.sort_values("issue_time").iloc[-1])
            if not f_s.empty
            else None
        )
        storms.append(
            {
                "atcf_id": atcf_id.upper(),
                "name": storm["name"],
                "file": f"storms/{atcf_id.upper()}.json",
                "first_issue_time": _iso(f_s["issue_time"].min()),
                "last_issue_time": _iso(last),
                "n_advisories": int(len(f_s)),
                "min_dist_km": _num(
                    pd.concat([f_s["min_dist"], o_s.get("min_dist")]).min()
                    if not f_s.empty
                    else None,
                    1,
                ),
                "active": bool(
                    last is not None
                    and now - last <= pd.Timedelta(hours=ACTIVE_HRS)
                ),
                "activations": storm["activations"],
                "latest": latest_row,
            }
        )
    storms.sort(key=lambda s: s["last_issue_time"] or "", reverse=True)

    # tables and rain series
    store.write_csv("fcast_monitoring.csv", _fcast_table(fc))
    ob_out = ob.copy()
    ob_out["issue_time"] = ob_out["issue_time"].map(_iso)
    store.write_csv("obsv_monitoring.csv", ob_out)

    start = mu.MONITORING_START - pd.Timedelta(days=7)
    g_all = df_gefs_all[df_gefs_all["issue_date"] >= start]
    gefs_rows = []
    for d, _ in g_all.groupby("issue_date"):
        s = _gefs_series(df_gefs_all, d)
        gefs_rows.append(s[["issue_date", "valid_date", "mean", "roll2_sum"]])
    gefs_tab = (
        pd.concat(gefs_rows).rename(
            columns={"mean": "mean_mm", "roll2_sum": "sum_2day_mm"}
        )
        if gefs_rows
        else pd.DataFrame()
    )
    store.write_csv("rain/chirps_gefs.csv", gefs_tab)
    im_tab = im[im["date"] >= start].rename(
        columns={"mean": "mean_mm", "roll2_sum": "sum_2day_mm"}
    )
    store.write_csv("rain/imerg.csv", im_tab)

    # index
    last_gefs = df_gefs_all["issue_date"].max()
    gefs_latest = _gefs_series(df_gefs_all, last_gefs).sort_values(
        "valid_date"
    )
    im_recent = im[im["date"] > im["date"].max() - pd.Timedelta(days=45)]
    index = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "season": season,
        "monitoring_start": _date(mu.MONITORING_START),
        "triggers": {
            s: {
                "name_fr": STAGE_NAMES_FR[s],
                "rain_mm": TRIGGERS[s]["rain_mm"],
                "lt_max_hrs": TRIGGERS[s]["lt_max_hrs"],
                "rp_years": TRIGGERS[s]["rp_years"],
            }
            for s in TRIGGERS
        },
        "overall_rp_years": OVERALL_RP_YEARS,
        "exposure_wind_kt": EXPOSURE_WIND_KT,
        "cutoff_hrs": LT_CUTOFF_HRS,
        "rain_distance_km": D_THRESH,
        "info_distance_km": MIN_EMAIL_DISTANCE,
        "sources": {
            "last_advisory_issue_time": _iso(latest_issue),
            "chirps_gefs_last_issue_date": _date(last_gefs),
            "imerg_last_date": _date(im["date"].max()),
            "last_obsv_row_issue_time": _iso(ob["issue_time"].max())
            if not ob.empty
            else None,
        },
        "season_activations": _activations(
            fc[fc["issue_time"].dt.year == season],
            ob[ob["issue_time"].dt.year == season],
        ),
        "storms": storms,
        "chirps_gefs_latest": {
            "issue_date": _date(last_gefs),
            "days": [
                {
                    "date": _date(r["valid_date"]),
                    "mean_mm": _num(r["mean"], 2),
                    "sum_2day_mm": _num(r["roll2_sum"], 2),
                }
                for _, r in gefs_latest.iterrows()
            ],
        },
        "imerg_recent": [
            {
                "date": _date(r["date"]),
                "mean_mm": _num(r["mean"], 2),
                "sum_2day_mm": _num(r["roll2_sum"], 2),
            }
            for _, r in im_recent.iterrows()
        ],
        "files": {
            "fcast_monitoring": "fcast_monitoring.csv",
            "obsv_monitoring": "obsv_monitoring.csv",
            "chirps_gefs": "rain/chirps_gefs.csv",
            "imerg": "rain/imerg.csv",
            "geo": "geo/hti.geojson",
            "manifest": "manifest.json",
        },
    }
    store.write_json("geo/hti.geojson", _geo_layers(adm0))
    store.write_json("index.json", index)
    store.finish(generated_at)
    logger.info(
        f"Site data exported ({len(store.manifest)} files, "
        f"{sum(v['bytes'] for v in store.manifest.values()) / 1e6:.1f} MB)."
    )
