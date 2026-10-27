"""Data behind the public monitoring page (``/2026/suivi/`` on the Pages site).

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
    fcast_monitoring.parquet        the same tables, typed, for scripts
    obsv_monitoring.parquet
    buffers/fcast_64kt.parquet      EXACT wind geometries behind every
                                    forecast row (GeoParquet, full
                                    precision): per stage the capped
                                    forecast buffer, the observed swath
                                    subtracted and the difference that
                                    exp_fcast_{lt}h counts
    buffers/obsv_64kt_swaths.parquet
                                    every cumulative observed swath of the
                                    season's storms, as the storms DB holds it
    geo/hti_adm0.parquet            the boundary exposure is counted inside
    datapackage.json                every file with its fields (Frictionless
                                    Data Package)
    manifest.json                   sha256 + size of every file, written last

Files live in dev blob under ``SITE_PREFIX``; the Pages workflow downloads
them and checks them against the manifest (pipelines/sync_site_data.py).
Advisory files are written once; everything else is rewritten each run.

Trigger values are always the ones the monitoring RECORDED (the v2
parquets), never recomputed ones. The wind geometries are the ones the
monitoring saved when it evaluated the advisory
(mu.FCAST_BUFFERS_BLOB, ``source`` "evaluated"). Advisories evaluated
before it saved them are rebuilt once by the function the monitoring
itself uses (``process_fcast_advisory(detail=True)``, pinned to the
recorded CHIRPS-GEFS issuance; ``source`` "rebuilt") and kept, so later
runs never recompute them. Rain windows and the track shown on the map
always come from that rebuild; any difference from the recorded values
is written into the advisory file. The .geojson files are simplified
for display; the .parquet files are exact.
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
from azure.core.exceptions import ResourceNotFoundError
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
from src.monitoring import exposure
from src.monitoring import monitoring_utils as mu
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

SITE_PREFIX = f"{blob.PROJECT_PREFIX}/monitoring/site"
SCHEMA_VERSION = 1

# Display simplification of the .geojson polygons (degrees, ~100 m) and
# coordinate rounding (4 decimals, ~11 m). Exposure values are computed on
# the full-resolution geometry, which the .parquet files below carry.
SIMPLIFY_DEG = 0.001
COORD_DECIMALS = 4

BUFFERS_PATH = "buffers/fcast_64kt.parquet"
SWATHS_PATH = "buffers/obsv_64kt_swaths.parquet"
ADM0_PATH = "geo/hti_adm0.parquet"
PARQUET_CT = "application/vnd.apache.parquet"
SOURCE_RANK = {"none": 0, "rebuilt": 1, "evaluated": 2}

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

    def write_parquet(self, path: str, df: pd.DataFrame):
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        self.write(path, buf.getvalue(), PARQUET_CT)

    def read_geoparquet(self, path: str) -> gpd.GeoDataFrame | None:
        """A GeoParquet file from the last export, or None."""
        if path not in self.manifest:
            return None
        return gpd.read_parquet(io.BytesIO(self.read(path)))

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
    # Same 2-day sums as update_obsv_monitoring (calendar days).
    return imerg.add_roll2_sum(imerg.load_imerg_from_postgres(recent=True))


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


def rebuild_advisory(rec: pd.Series, df_gefs_all: pd.DataFrame, adm0_3857):
    """process_fcast_advisory(detail=True) for a recorded row, pinned to
    its recorded CHIRPS-GEFS issuance; None if no longer rebuildable."""
    gefs_date = rec.get("gefs_issue_date")
    gefs_date = None if gefs_date is None or pd.isna(gefs_date) else gefs_date
    return mu.process_fcast_advisory(
        rec["atcf_id"],
        pd.Timestamp(rec["issue_time"]),
        df_gefs_all,
        adm0_3857,
        name=rec["name"],
        detail=True,
        gefs_issue_date=gefs_date,
    )


def _layer_geom(gdf: gpd.GeoDataFrame | None, stage: str, layer: str):
    """Geometry of one stage/layer buffer record, or None."""
    if gdf is None or gdf.empty:
        return None
    m = gdf[(gdf["stage"] == stage) & (gdf["layer"] == layer)]
    if m.empty:
        return None
    g = m.geometry.iloc[0]
    return None if g is None or g.is_empty else g


def _same_geom(a, b) -> bool:
    """Identical geometries, coordinate for coordinate."""
    a = None if a is None or a.is_empty else a
    b = None if b is None or b.is_empty else b
    if a is None or b is None:
        return a is None and b is None
    return shapely.to_wkb(a) == shapely.to_wkb(b)


def build_advisory(
    rec: pd.Series,
    res,
    adm0_3857,
    evaluated: gpd.GeoDataFrame | None = None,
) -> dict:
    """GeoJSON FeatureCollection for one recorded advisory row.

    ``res`` is ``rebuild_advisory``'s result; ``evaluated`` the buffer
    records the monitoring saved for this row, if any. The buffers shown
    are the evaluated ones when they exist, else the rebuilt ones.
    """
    atcf_id, issue_time = rec["atcf_id"], pd.Timestamp(rec["issue_time"])
    has_ev = evaluated is not None and not evaluated.empty
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
    elif not has_ev:
        logger.warning(
            f"{rec['monitor_id']}: advisory no longer rebuildable from the "
            "storms DB; publishing the recorded values without geometry."
        )

    # capped forecast buffers (forecast-only: observed swath removed), as
    # evaluated when the monitoring saved them, else as rebuilt
    identical = {}
    for stage in mu.FCAST_STAGES:
        lt = TRIGGERS[stage]["lt_max_hrs"]
        rebuilt_geom = (
            res[1]["buffers"][stage]["geometry"] if res is not None else None
        )
        geom = rebuilt_geom
        if has_ev:
            geom = _layer_geom(evaluated, stage, "fcastonly")
            if res is not None:
                identical[stage] = _same_geom(geom, rebuilt_geom)
        g = _geojson_geom(geom)
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
                    "pop_exposed_hti": _num(rec.get(f"exp_fcast_{lt}h"), 0),
                },
            }
        )
    if identical:
        rebuilt["geometry_identical_to_evaluated"] = identical

    # observed swath at issuance (what exp_obsv counts). The forecast
    # buffers only had a swath removed when one existed at exactly the
    # issue time (fetch_obsv_buffer_at, as in ds-storms-pipeline); flag it.
    sw_time, sw_geom = _fetch_obsv_buffer_latest(atcf_id, issue_time)
    subtracted = None
    if has_ev:
        subtracted = any(
            _layer_geom(evaluated, s, "obsv_swath_subtracted") is not None
            for s in mu.FCAST_STAGES
        )
    elif res is not None:
        subtracted = any(
            res[1]["buffers"][s]["obsv_geometry"] is not None
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
        "buffers_source": (
            "evaluated" if has_ev else "rebuilt" if res is not None else None
        ),
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


# --- exact files (for scripts) ---------------------------------------------


def _utc(col: pd.Series) -> pd.Series:
    t = pd.to_datetime(col)
    return (
        t.dt.tz_localize("UTC") if t.dt.tz is None else t.dt.tz_convert("UTC")
    )


def _load_evaluated() -> gpd.GeoDataFrame | None:
    """The buffers the monitoring saved when it evaluated each advisory."""
    try:
        return mu.load_fcast_buffers()
    except ResourceNotFoundError:
        return None


def _no_buffer_records(rec: pd.Series) -> list[dict]:
    """Null-geometry records for an advisory evaluated before the
    monitoring saved its buffers and no longer rebuildable."""
    keys = ["geometry", "fcast_geometry", "obsv_geometry", "pop_exposed"]
    det = {"buffers": {s: dict.fromkeys(keys) for s in mu.FCAST_STAGES}}
    recs = mu.buffer_records(rec, det, "none")
    for r in recs:
        r.update(ocha_lens_version=None, pop_raster=None, adm0_boundary=None)
    return recs


BUFFER_COLS = [
    "monitor_id",
    "atcf_id",
    "name",
    "issue_time",
    "stage",
    "lt_max_hrs",
    "wind_speed_kt",
    "layer",
    "source",
    "pop_exposed_hti",
    "pop_exposed_recorded",
    "ocha_lens_version",
    "pop_raster",
    "adm0_boundary",
    "geometry",
]


def _buffers_table(fc: pd.DataFrame, parts: list) -> gpd.GeoDataFrame:
    """One record per recorded forecast row, stage and layer: the
    evaluated geometry where saved, else the rebuilt one, else none."""
    frames = []
    for p in parts:
        if p is None or len(p) == 0:
            continue
        p = pd.DataFrame(p).drop(
            columns=["name", "pop_exposed_recorded"], errors="ignore"
        )
        p["issue_time"] = _utc(p["issue_time"])
        frames.append(p)
    gdf = gpd.GeoDataFrame(
        pd.concat(frames, ignore_index=True), geometry="geometry", crs=4326
    )
    gdf["_rank"] = gdf["source"].map(SOURCE_RANK)
    gdf = (
        gdf.sort_values("_rank", kind="stable")
        .drop_duplicates(["monitor_id", "stage", "layer"], keep="last")
        .drop(columns="_rank")
    )
    rec = fc.drop_duplicates("monitor_id", keep="last").set_index("monitor_id")
    gdf = gdf[gdf["monitor_id"].isin(rec.index)].copy()
    gdf["atcf_id"] = gdf["atcf_id"].str.upper()
    gdf["name"] = gdf["monitor_id"].map(rec["name"])
    gdf["pop_exposed_recorded"] = [
        rec.at[m, f"exp_fcast_{lt}h"] if layer == "fcastonly" else np.nan
        for m, lt, layer in zip(
            gdf["monitor_id"], gdf["lt_max_hrs"], gdf["layer"]
        )
    ]
    gdf.loc[gdf.geometry.notna() & gdf.geometry.is_empty, "geometry"] = None
    return gdf[BUFFER_COLS].sort_values(
        ["issue_time", "atcf_id", "stage", "layer"]
    )


def _swaths_table(atcf_ids) -> gpd.GeoDataFrame:
    """Every cumulative observed 64 kt swath the storms DB holds for these
    storms, exactly as stored, with the HTI exposure the storms pipeline
    counted inside it (what exp_obsv / obsv_exp report)."""
    ids = tuple(sorted({a.upper() for a in atcf_ids}))
    query = text(
        """
        SELECT atcf_id, valid_time, wind_speed_kt, geometry
        FROM storms.nhc_tracks_obsv_buffers
        WHERE atcf_id IN :ids AND wind_speed_kt = :kt
        ORDER BY atcf_id, valid_time
        """
    )
    with storms_db.get_engine().connect() as conn:
        gdf = gpd.read_postgis(
            query,
            conn,
            params={"ids": ids, "kt": EXPOSURE_WIND_KT},
            geom_col="geometry",
        )
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    exp = pd.concat(
        [storms_db.fetch_obsv_exposure(a).assign(atcf_id=a) for a in ids]
    )
    exp = exp[exp["wind_speed_kt"] == EXPOSURE_WIND_KT]
    gdf = gdf.merge(
        exp[["atcf_id", "valid_time", "pop_exposed"]].rename(
            columns={"pop_exposed": "pop_exposed_hti"}
        ),
        on=["atcf_id", "valid_time"],
        how="left",
    )
    gdf["valid_time"] = _utc(gdf["valid_time"])
    return gdf[
        [
            "atcf_id",
            "valid_time",
            "wind_speed_kt",
            "pop_exposed_hti",
            "geometry",
        ]
    ]


def _adm0_table() -> gpd.GeoDataFrame:
    adm0 = exposure.load_hti_adm0()
    return gpd.GeoDataFrame(
        {
            "name": ["Haiti"],
            "source": [exposure.provenance()["adm0_boundary"]],
        },
        geometry=[adm0.geometry.union_all()],
        crs=adm0.crs,
    )


FIELD_DOCS = {
    "monitor_id": "Id of the evaluation: {atcf_id}_{fcast|obsv}_{time}.",
    "atcf_id": "NHC storm id (ATCF).",
    "name": "Storm name.",
    "issue_time": "Forecast rows: NHC advisory issue time. Observation "
    "rows: when the IMERG day became available (day + 1, 15:00 UTC). UTC.",
    "hours_to_closest_pass": "Hours from the advisory to the closest pass "
    "to Haiti forecast by it.",
    "gefs_issue_date": "CHIRPS-GEFS issuance used: the latest issued "
    "(08:50 UTC) before the advisory.",
    "closest_s": "Forecast wind speed (kt) at the closest pass.",
    "past_cutoff": "True when the closest pass is forecast under 48 h "
    "away: the advisory can no longer trigger.",
    "min_dist": "Minimum distance (km) from the track to Haiti (forecast "
    "rows: forecast track; observation rows: observed track so far).",
    "exp_obsv": "People in Haiti inside the observed >= 64 kt swath at the "
    "advisory, as counted by ds-storms-pipeline (see "
    "buffers/obsv_64kt_swaths.parquet).",
    "rain_120h": "Max 2-day national-mean CHIRPS-GEFS rain (mm) over the "
    "days the track is forecast within 230 km of Haiti within 120 h, plus "
    "the next day. Empty when it is not forecast there.",
    "rain_72h": "As rain_120h, within 72 h.",
    "exp_fcast_120h": "People in Haiti (WorldPop 2026, 1 km) inside the "
    ">= 64 kt forecast buffer capped at 120 h, observed swath removed: the "
    "'fcastonly' geometry in buffers/fcast_64kt.parquet, counted inside "
    "geo/hti_adm0.parquet.",
    "exp_fcast_72h": "As exp_fcast_120h, capped at 72 h.",
    "exp_total_120h": "exp_fcast_120h + exp_obsv; the wind condition is "
    "met when above 0.",
    "exp_total_72h": "exp_fcast_72h + exp_obsv.",
    "rain_trigger_mobilisation": "Rain condition met for Mobilisation.",
    "exp_trigger_mobilisation": "Wind condition met for Mobilisation.",
    "mobilisation_trigger": "Mobilisation triggered by this advisory "
    "(cutoff applied).",
    "rain_trigger_action": "Rain condition met for Action.",
    "exp_trigger_action": "Wind condition met for Action.",
    "action_trigger": "Action triggered by this advisory (cutoff applied).",
    "file": "The advisory's GeoJSON file (geometry simplified for display).",
    "obsv_rain": "Max 2-day national-mean IMERG rain (mm) over the days the "
    "observed track was within 230 km of Haiti, plus the next day.",
    "obsv_exp": "People in Haiti inside the observed >= 64 kt swath so far.",
    "rainfall_relevant": "True while the IMERG day is within that window.",
    "rain_trigger_obsv": "Observed rain condition met (Réponse précoce).",
    "exp_trigger_obsv": "Observed wind condition met.",
    "obsv_trigger": "Réponse précoce conditions met on this day.",
    "stage": "mobilisation (forecast capped at 120 h) or action (72 h).",
    "lt_max_hrs": "Lead-time cap of the stage (hours).",
    "wind_speed_kt": "Wind threshold of the buffer (kt).",
    "layer": "fcast_capped: >= 64 kt buffer of the forecast track points up "
    "to lt_max_hrs (30-min interpolation, quadrant radii; ocha-lens). "
    "obsv_swath_subtracted: the observed swath at exactly the issue time, "
    "removed from it (null when none existed). fcastonly: the difference, "
    "which exp_fcast_{lt}h counts.",
    "source": "evaluated: saved by the monitoring run that recorded the "
    "row. rebuilt: recomputed later with the same function "
    "(ocha_lens_version is then the export's). none: no longer "
    "rebuildable (advisories scored before 2026-09-22 whose storms-DB issue "
    "time moved); geometry null.",
    "pop_exposed_hti": "People in Haiti counted inside this geometry "
    "(fcastonly rows; swaths: by ds-storms-pipeline).",
    "pop_exposed_recorded": "The row's recorded exp_fcast_{lt}h "
    "(fcastonly rows). Equal to pop_exposed_hti when source is evaluated.",
    "ocha_lens_version": "ocha-lens version that built the buffer.",
    "pop_raster": "WorldPop raster exposure is counted on.",
    "adm0_boundary": "Boundary exposure is counted inside "
    "(geo/hti_adm0.parquet).",
    "geometry": "EPSG:4326, full precision (GeoParquet WKB).",
    "valid_time": "Time of the observed position the cumulative swath runs "
    "to (UTC).",
    "source_boundary": "Boundary source.",
    "issue_date": "CHIRPS-GEFS issue date.",
    "valid_date": "Day the rain value is for.",
    "date": "IMERG day.",
    "mean_mm": "National-mean daily rain (mm).",
    "sum_2day_mm": "Rain of the day before + this day (mm), the 2-day sum "
    "the triggers use.",
}


def _field_type(dtype) -> str:
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_integer_dtype(dtype):
        return "integer"
    if pd.api.types.is_float_dtype(dtype):
        return "number"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    if str(dtype) == "geometry":
        return "any"
    return "string"


def _datapackage(store: SiteStore, tables: dict, generated_at: str) -> dict:
    """Frictionless Data Package: every file, with the fields of each
    table taken from the frame written."""
    resources = []
    for path, meta in sorted(store.manifest.items()):
        if path in ("datapackage.json", "manifest.json") or path.startswith(
            ("advisories/", "storms/")
        ):
            continue
        fmt = path.rsplit(".", 1)[-1]
        r = {
            "name": path.replace("/", "-").replace("_", "-").lower(),
            "path": path,
            "format": fmt,
            "bytes": meta["bytes"],
            "hash": f"sha256:{meta['sha256']}",
        }
        if path in tables:
            desc, df = tables[path]
            r["description"] = desc
            r["schema"] = {
                "fields": [
                    {
                        "name": c,
                        "type": _field_type(df[c].dtype),
                        "description": FIELD_DOCS.get(c, ""),
                    }
                    for c in df.columns
                ]
            }
        resources.append(r)
    return {
        "name": "hti-hurricanes-monitoring-2026",
        "title": "Haiti hurricanes anticipatory action: 2026 trigger "
        "monitoring data",
        "description": "Everything the 2026 Haiti hurricane trigger "
        "monitoring recorded, with the exact wind geometries behind each "
        "exposure value. Per-advisory GeoJSON (advisories/{ATCF}/"
        "{YYYY-MM-DDTHHMMZ}.geojson, simplified for display) and per-storm "
        "JSON (storms/{ATCF}.json) are listed in index.json.",
        "created": generated_at,
        "homepage": "https://ocha-dap.github.io/ds-aa-hti-hurricanes/2026/suivi/",
        "sources": [
            {"title": "NHC advisories via ds-storms-pipeline"},
            {"title": "CHIRPS-GEFS (UCSB Climate Hazards Center)"},
            {"title": "IMERG (NASA GPM)"},
            {"title": "WorldPop 2026 constrained 1 km"},
            {"title": "FieldMaps edge-matched humanitarian boundaries"},
        ],
        "resources": resources,
    }


def export_site(local_dir: str | None = None, full: bool = False):
    """Write the site data. ``full`` rewrites every advisory file and
    rebuilds every geometry the monitoring did not save."""
    generated_at = _iso(datetime.now(timezone.utc))
    store = SiteStore(local_dir)
    adm0 = codab.load_codab_from_blob().to_crs(3857)
    fc, ob = _load_recorded()
    if fc.empty:
        logger.info("No forecast monitoring rows yet; nothing to export.")
        return
    df_gefs_all = mu.load_gefs_with_issue_times()
    im = _load_imerg()

    # Advisory files (immutable; only the missing ones) and the exact
    # buffers: as evaluated where the monitoring saved them, else rebuilt
    # once and kept from the last export.
    evaluated = _load_evaluated()
    ev_by_id = (
        dict(tuple(evaluated.groupby("monitor_id")))
        if evaluated is not None
        else {}
    )
    kept = None if full else store.read_geoparquet(BUFFERS_PATH)
    kept_ids = set(kept["monitor_id"]) if kept is not None else set()
    have = set() if full else store.existing("advisories/")
    n_new, rebuilt_recs = 0, []
    for _, rec in fc.sort_values("issue_time").iterrows():
        mid = rec["monitor_id"]
        path = (
            f"advisories/{rec['atcf_id'].upper()}/"
            f"{stamp(rec['issue_time'])}.geojson"
        )
        need_file = not (path in have and path in store.manifest)
        need_buf = mid not in ev_by_id and mid not in kept_ids
        if not (need_file or need_buf):
            continue
        res = rebuild_advisory(rec, df_gefs_all, adm0)
        if need_file:
            store.write_json(
                path, build_advisory(rec, res, adm0, ev_by_id.get(mid))
            )
            n_new += 1
        if need_buf:
            rebuilt_recs += (
                mu.buffer_records(rec, res[1], "rebuilt")
                if res is not None
                else _no_buffer_records(rec)
            )
    logger.info(
        f"Advisory files written: {n_new}; buffers rebuilt for "
        f"{len(rebuilt_recs) // (2 * len(mu.BUFFER_LAYERS))} advisories"
    )
    buffers = _buffers_table(fc, [kept, rebuilt_recs, evaluated])
    store.write_parquet(BUFFERS_PATH, buffers)

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

    # tables and rain series (CSV, and Parquet with types for scripts)
    fc_tab = _fcast_table(fc)
    store.write_csv("fcast_monitoring.csv", fc_tab)
    ob_out = ob.copy()
    ob_out["issue_time"] = ob_out["issue_time"].map(_iso)
    store.write_csv("obsv_monitoring.csv", ob_out)
    fc_pq = fc_tab.assign(issue_time=_utc(fc["issue_time"]).to_numpy())
    if "gefs_issue_date" in fc_pq:
        fc_pq["gefs_issue_date"] = pd.to_datetime(fc_pq["gefs_issue_date"])
    ob_pq = ob.assign(issue_time=_utc(ob["issue_time"]))
    store.write_parquet("fcast_monitoring.parquet", fc_pq)
    store.write_parquet("obsv_monitoring.parquet", ob_pq)
    swaths = _swaths_table(set(fc["atcf_id"]) | set(ob["atcf_id"]))
    store.write_parquet(SWATHS_PATH, swaths)
    adm0_exact = _adm0_table()
    store.write_parquet(ADM0_PATH, adm0_exact)

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
            "fcast_monitoring_parquet": "fcast_monitoring.parquet",
            "obsv_monitoring_parquet": "obsv_monitoring.parquet",
            "fcast_buffers_exact": BUFFERS_PATH,
            "obsv_swaths_exact": SWATHS_PATH,
            "adm0_exact": ADM0_PATH,
            "chirps_gefs": "rain/chirps_gefs.csv",
            "imerg": "rain/imerg.csv",
            "geo": "geo/hti.geojson",
            "datapackage": "datapackage.json",
            "manifest": "manifest.json",
        },
    }
    store.write_json("geo/hti.geojson", _geo_layers(adm0))
    store.write_json("index.json", index)
    tables = {
        "fcast_monitoring.parquet": (
            "One row per NHC advisory: every forecast value and result, "
            "as recorded.",
            fc_pq,
        ),
        "fcast_monitoring.csv": ("Same as fcast_monitoring.parquet.", fc_tab),
        "obsv_monitoring.parquet": (
            "One row per storm and IMERG day: observed rain and wind, as "
            "recorded.",
            ob_pq,
        ),
        "obsv_monitoring.csv": ("Same as obsv_monitoring.parquet.", ob_out),
        BUFFERS_PATH: (
            "EXACT wind geometries behind each forecast row's "
            "exp_fcast_{lt}h (GeoParquet, EPSG:4326, full precision): one "
            "record per advisory, stage and layer.",
            buffers,
        ),
        SWATHS_PATH: (
            "Every cumulative observed >= 64 kt swath of the season's "
            "storms, exactly as the storms DB holds it.",
            swaths,
        ),
        ADM0_PATH: (
            "The boundary exposure is counted inside (exact).",
            adm0_exact,
        ),
        "rain/chirps_gefs.csv": (
            "Every CHIRPS-GEFS issuance since monitoring start: "
            "national-mean daily rain and 2-day sum.",
            gefs_tab,
        ),
        "rain/imerg.csv": (
            "IMERG national-mean daily rain and 2-day sum.",
            im_tab,
        ),
    }
    store.write_json(
        "datapackage.json", _datapackage(store, tables, generated_at)
    )
    store.finish(generated_at)
    logger.info(
        f"Site data exported ({len(store.manifest)} files, "
        f"{sum(v['bytes'] for v in store.manifest.values()) / 1e6:.1f} MB)."
    )
