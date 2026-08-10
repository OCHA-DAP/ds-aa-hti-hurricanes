"""Decide which emails are due, build them, send via Listmonk.

Email types (email_record_v2.csv in blob dedups):
- info: one per (storm, advisory) while the storm is within
  MIN_EMAIL_DISTANCE of Haiti; flagged past-cutoff after the 48 h
  cutoff (and never presented as an activation).
- mobilisation / action / obsv: one per storm per stage, when the
  stage's trigger fires.

The obsv trigger email is suppressed when the Action stage already
fired for that storm (carried over from the v1 system).
"""

import traceback

import pandas as pd

from src.constants import MIN_EMAIL_DISTANCE, STAGE_NAMES_FR
from src.datasources import codab, storms_db
from src.email import body, plots, send
from src.email.plots import fr_datetime
from src.monitoring import monitoring_utils
from src.utils import blob
from src.utils.logging import get_logger

logger = get_logger(__name__)

EMAIL_RECORD_BLOB = f"{blob.PROJECT_PREFIX}/email/email_record_v2.csv"

WSP_UNAVAILABLE_HTML = (
    "<p style='color:#7e8e8f'>(Prévisions probabilistes WSP non "
    "disponibles pour cette prévision.)</p>"
)


def load_email_record() -> pd.DataFrame:
    try:
        return blob.load_csv_from_blob(EMAIL_RECORD_BLOB)
    except Exception:
        logger.info("No existing v2 email record; starting fresh.")
        return pd.DataFrame(columns=["monitor_id", "atcf_id", "email_type"])


def save_email_record(df: pd.DataFrame):
    blob.upload_csv_to_blob(EMAIL_RECORD_BLOB, df)


def _already_sent(record: pd.DataFrame, email_type: str, key: str, by: str):
    return key in record[record["email_type"] == email_type][by].unique()


def build_email_plots(
    atcf_id: str, issue_time, name: str
) -> tuple[str, str | None, str | None]:
    """(wsp_img, wsp_map_img, det_map_img) HTML for one advisory."""
    wsp_img = WSP_UNAVAILABLE_HTML
    map_img = None
    det_img = None
    try:
        df_wsp = storms_db.fetch_wsp_exposure(atcf_id, issue_time)
        if not df_wsp.empty:
            df_o = storms_db.fetch_obsv_exposure(atcf_id)
            df_o = df_o[df_o["valid_time"] <= storms_db.naive_utc(issue_time)]
            floors = (
                df_o.sort_values("valid_time")
                .groupby("wind_speed_kt")["pop_exposed"]
                .last()
                .to_dict()
            )
            wsp_img = plots.wsp_density_img(
                df_wsp,
                name,
                issue_time,
                obsv_floor_by_kt=floors,
                total_pop=storms_db.fetch_hti_population(),
            )
    except Exception as e:
        logger.error(f"Could not build WSP plot for {atcf_id}: {e}")
        traceback.print_exc()
    try:
        gdf_all = storms_db.fetch_tracks_geo(atcf_id)
        t = storms_db.naive_utc(issue_time)
        tracks_obsv = gdf_all[
            (gdf_all["leadtime"] == 0) & (gdf_all["issued_time"] <= t)
        ]
        tracks_fcast = gdf_all[
            (gdf_all["issued_time"] == t) & (gdf_all["leadtime"] > 0)
        ]
        wsp_polys = None
        try:
            wsp_polys = storms_db.fetch_wsp_polygons(
                atcf_id, issue_time, wind_threshold_kt=64
            )
        except Exception:
            pass
        if not (tracks_obsv.empty and tracks_fcast.empty):
            adm0 = codab.load_codab_from_blob()
            map_img = plots.storm_map_img(
                tracks_obsv,
                tracks_fcast,
                wsp_polys,
                adm0,
                name,
                issue_time,
            )
            fcast_buffers = storms_db.fetch_fcast_buffers(atcf_id, issue_time)
            _, obsv_geom = storms_db.fetch_obsv_buffer_at(atcf_id, issue_time)
            det_img = plots.det_map_img(
                tracks_obsv,
                tracks_fcast,
                fcast_buffers,
                obsv_geom,
                adm0,
                name,
                issue_time,
            )
            timing = _compute_timing(tracks_fcast, adm0.to_crs(3857))
            det_img += _timing_html(timing, issue_time)
    except Exception as e:
        logger.error(f"Could not build map for {atcf_id}: {e}")
        traceback.print_exc()
    return wsp_img, map_img, det_img


def _compute_timing(tracks_fcast, adm0_3857) -> dict:
    """Timing estimates from the deterministic forecast: closest-pass
    time and the earliest time each wind level could reach Haiti.

    Arrival is estimated per interpolated track point as the first time
    the distance to Haiti drops below that point's largest quadrant
    wind radius — an approximation (radii are asymmetric and the swath
    is what actually matters), so present as an estimate.
    """
    import geopandas as gpd
    from ocha_lens.utils.storm import expand_quad_col

    if tracks_fcast is None or tracks_fcast.empty:
        return {}
    gdf = tracks_fcast.copy()
    for kt in (34, 64):
        gdf = expand_quad_col(gdf, f"quadrant_radius_{kt}")
        quad_cols = [
            f"quadrant_radius_{kt}_{q}" for q in ("ne", "se", "sw", "nw")
        ]
        gdf[f"r{kt}_km"] = gdf[quad_cols].max(axis=1).fillna(0) * 1.852
    df = (
        pd.DataFrame(
            {
                "valid_time": gdf["valid_time"],
                "lon": gdf.geometry.x,
                "lat": gdf.geometry.y,
                "r34_km": gdf["r34_km"],
                "r64_km": gdf["r64_km"],
            }
        )
        .drop_duplicates(subset=["valid_time"])
        .set_index("valid_time")
        .resample("30min")
        .interpolate()
        .reset_index()
    )
    pts = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
        crs="EPSG:4326",
    ).to_crs(3857)
    dist_km = pts.geometry.distance(adm0_3857.iloc[0].geometry) / 1000

    timing = {}
    idx = dist_km.idxmin()
    timing["closest_time"] = df.loc[idx, "valid_time"]
    timing["closest_dist_km"] = float(dist_km[idx])
    for kt in (34, 64):
        hit = df.loc[dist_km <= df[f"r{kt}_km"], "valid_time"]
        timing[f"arrival{kt}"] = hit.min() if not hit.empty else None
    return timing


def _timing_html(timing: dict, issue_time) -> str:
    """'Earliest time to impact' panel shown under the deterministic
    map."""
    if not timing:
        return ""
    t0 = storms_db.naive_utc(issue_time)

    def _lead(ts) -> str:
        hrs = (storms_db.naive_utc(ts) - t0).total_seconds() / 3600
        return f"≈ {max(0, int(round(hrs)))} h"

    rows = []
    for kt in (34, 64):
        arr = timing.get(f"arrival{kt}")
        label = f"Arrivée la plus tôt possible des vents ≥ {kt} kt sur Haïti"
        if arr is None:
            value = "non prévue sur cet horizon"
        elif storms_db.naive_utc(arr) <= t0:
            value = "conditions possibles dès maintenant"
        else:
            value = f"{fr_datetime(arr)} ({_lead(arr)})"
        rows.append(f"<li>{label} : <b>{value}</b></li>")
    closest = timing.get("closest_time")
    if closest is not None:
        rows.append(
            "<li>Passage au plus près d'Haïti "
            f"({timing['closest_dist_km']:.0f} km) : "
            f"<b>{fr_datetime(closest)} ({_lead(closest)})</b></li>"
        )
    return (
        "<div style='background:#e8effb;border-left:3px solid #1862d8;"
        "padding:10px 14px;border-radius:0 6px 6px 0;margin:10px 0'>"
        "<b>Délais estimés</b> (d'après la trajectoire et les rayons de "
        "vent prévus ; estimation indicative)"
        "<ul style='margin:6px 0 0;padding-left:20px'>"
        + "".join(rows)
        + "</ul></div>"
    )


def _fcast_info_status(row: pd.Series) -> str:
    if row["past_cutoff"]:
        return "DÉLAI DÉPASSÉ"
    if row["action_trigger"]:
        return "ACTIVATION : ACTION"
    if row["mobilisation_trigger"]:
        return "ACTIVATION : MOBILISATION"
    return "PAS D'ACTIVATION"


def send_fcast_info_email(monitor_id: str, row: pd.Series):
    wsp_img, map_img, det_img = build_email_plots(
        row["atcf_id"], row["issue_time"], row["name"]
    )
    html = body.build_fcast_info_body(row, wsp_img, map_img, det_img)
    subject = (
        f"Action anticipatoire Haïti – {row['name']} : prévisions NHC du "
        f"{fr_datetime(row['issue_time'])} ({_fcast_info_status(row)})"
    )
    send.send_campaign(f"aa-hti-info-{monitor_id}", subject, html, "info")


def send_obsv_info_email(monitor_id: str, row: pd.Series):
    _, map_img, det_img = build_email_plots(
        row["atcf_id"], row["issue_time"], row["name"]
    )
    html = body.build_obsv_info_body(row, map_img or det_img)
    status = (
        "ACTIVATION : RÉPONSE PRÉCOCE"
        if row["obsv_trigger"]
        else "PAS D'ACTIVATION"
    )
    subject = (
        f"Action anticipatoire Haïti – {row['name']} : observations du "
        f"{fr_datetime(row['issue_time'])} ({status})"
    )
    send.send_campaign(f"aa-hti-info-{monitor_id}", subject, html, "info")


def send_trigger_email(monitor_id: str, row: pd.Series, stage: str):
    wsp_img, map_img, det_img = build_email_plots(
        row["atcf_id"], row["issue_time"], row["name"]
    )
    if stage == "obsv":
        wsp_img = None
    html = body.build_trigger_body(
        row, stage, wsp_img, (det_img or "") + (map_img or "")
    )
    subject = (
        f"Action anticipatoire Haïti – déclencheur "
        f"{STAGE_NAMES_FR[stage].upper()} ATTEINT pour {row['name']}"
    )
    send.send_campaign(f"aa-hti-{stage}-{monitor_id}", subject, html, stage)


def update_fcast_info_emails():
    df_monitoring = monitoring_utils.load_existing_monitoring_points("fcast")
    if df_monitoring.empty:
        logger.info("No monitoring points; nothing to send.")
        return
    record = load_email_record()
    new_records = []
    for monitor_id, row in df_monitoring.set_index("monitor_id").iterrows():
        if row["min_dist"] > MIN_EMAIL_DISTANCE:
            continue
        if _already_sent(record, "info", monitor_id, "monitor_id"):
            continue
        try:
            logger.info(f"sending info email for {monitor_id}")
            send_fcast_info_email(monitor_id, row)
            new_records.append(
                {
                    "monitor_id": monitor_id,
                    "atcf_id": row["atcf_id"],
                    "email_type": "info",
                }
            )
        except Exception as e:
            logger.error(f"could not send info email for {monitor_id}: {e}")
            traceback.print_exc()
    _append_and_save(record, new_records)


def update_obsv_info_emails():
    df_monitoring = monitoring_utils.load_existing_monitoring_points("obsv")
    if df_monitoring.empty:
        logger.info("No monitoring points; nothing to send.")
        return
    record = load_email_record()
    new_records = []
    for monitor_id, row in df_monitoring.set_index("monitor_id").iterrows():
        if row["min_dist"] > MIN_EMAIL_DISTANCE:
            continue
        if not row["rainfall_relevant"]:
            continue
        if _already_sent(record, "info", monitor_id, "monitor_id"):
            continue
        try:
            logger.info(f"sending obsv info email for {monitor_id}")
            send_obsv_info_email(monitor_id, row)
            new_records.append(
                {
                    "monitor_id": monitor_id,
                    "atcf_id": row["atcf_id"],
                    "email_type": "info",
                }
            )
        except Exception as e:
            logger.error(
                f"could not send obsv info email for {monitor_id}: {e}"
            )
            traceback.print_exc()
    _append_and_save(record, new_records)


def update_fcast_trigger_emails():
    df_monitoring = monitoring_utils.load_existing_monitoring_points("fcast")
    if df_monitoring.empty:
        logger.info("No monitoring points; nothing to send.")
        return
    record = load_email_record()
    new_records = []
    for atcf_id, group in df_monitoring.groupby("atcf_id"):
        for stage in ("mobilisation", "action"):
            if _already_sent(record, stage, atcf_id, "atcf_id"):
                continue
            triggered = group[group[f"{stage}_trigger"]]
            if triggered.empty:
                continue
            row = triggered.sort_values("issue_time").iloc[0]
            monitor_id = row["monitor_id"]
            try:
                logger.info(f"sending {stage} email for {monitor_id}")
                send_trigger_email(monitor_id, row, stage)
                new_records.append(
                    {
                        "monitor_id": monitor_id,
                        "atcf_id": atcf_id,
                        "email_type": stage,
                    }
                )
            except Exception as e:
                logger.error(
                    f"could not send {stage} email for {monitor_id}: {e}"
                )
                traceback.print_exc()
    _append_and_save(record, new_records)


def update_obsv_trigger_emails():
    df_monitoring = monitoring_utils.load_existing_monitoring_points("obsv")
    if df_monitoring.empty:
        logger.info("No monitoring points; nothing to send.")
        return
    record = load_email_record()
    new_records = []
    for atcf_id, group in df_monitoring.groupby("atcf_id"):
        if _already_sent(record, "obsv", atcf_id, "atcf_id"):
            continue
        if _already_sent(record, "action", atcf_id, "atcf_id"):
            logger.info(
                f"action already fired for {atcf_id}; skipping obsv email"
            )
            continue
        triggered = group[group["obsv_trigger"]]
        if triggered.empty:
            continue
        row = triggered.sort_values("issue_time").iloc[0]
        monitor_id = row["monitor_id"]
        try:
            logger.info(f"sending obsv email for {monitor_id}")
            send_trigger_email(monitor_id, row, "obsv")
            new_records.append(
                {
                    "monitor_id": monitor_id,
                    "atcf_id": atcf_id,
                    "email_type": "obsv",
                }
            )
        except Exception as e:
            logger.error(f"could not send obsv email for {monitor_id}: {e}")
            traceback.print_exc()
    _append_and_save(record, new_records)


def _append_and_save(record: pd.DataFrame, new_records: list):
    if not new_records:
        logger.info("No new emails sent.")
        return
    if send.DRY_RUN:
        logger.info(f"DRY_RUN: not recording {len(new_records)} sent emails.")
        return
    combined = pd.concat(
        [record, pd.DataFrame(new_records)], ignore_index=True
    )
    save_email_record(combined)
