"""Every trigger pathway, per storm, on one footing.

For each storm in the Haiti set the framework's indicators are reduced to
one number, evaluated the way the monitoring does (per NHC advisory,
pre-cutoff issuances only):

- ``fcast_exp_64``  - population exposed to >= 64 kt forecast winds
                      (future-only cone + observed swath so far), max over
                      pre-cutoff advisories
- ``fcast_rain_mm`` - national-mean CHIRPS-GEFS 2-day rainfall from the
                      GEFS issuance most recent before each pre-cutoff
                      advisory, over the dates the forecast track is
                      within D_THRESH (mobilisation lead cap)
- ``n_orange``      - departments DGPC would have placed in orange on the
                      same forecasts (gust wind or pixel rain), from
                      pre-cutoff advisories only
- ``obsv_exp_64``   - population inside the observed >= 64 kt swath
- ``obsv_rain_mm``  - national-mean IMERG 2-day rainfall in the window

so that thresholds can be moved and the activation record and return
periods recomputed without touching the source data.
"""

import geopandas as gpd
import numpy as np
import pandas as pd
from sqlalchemy import text

from src.constants import D_THRESH, EXPOSURE_WIND_KT, LT_CUTOFF_HRS, TRIGGERS
from src.datasources import chirps_gefs, codab, imerg
from src.datasources import storms_db as sdb
from src.dgpc import constants as dc
from src.dgpc.dept_forecast import wind_forecast_by_dept
from src.monitoring.monitoring_utils import _interp_track
from src.utils import blob

PREFIX = f"{blob.PROJECT_PREFIX}/processed/dgpc"
N_YEARS = dc.SEASON_END - dc.SEASON_START + 1

TRACK_Q = """
SELECT atcf_id, issued_time, valid_time, leadtime, wind_speed, geometry
FROM storms.nhc_tracks_geo
WHERE provider = 'nhc' AND leadtime IS NOT NULL AND atcf_id IN :ids
ORDER BY atcf_id, issued_time, leadtime
"""
EXP_Q = """
SELECT atcf_id, issued_time, pop_exposed
FROM storms.nhc_tracks_fcastonly_exposure
WHERE iso3 = 'HTI' AND admin_level = 0 AND wind_speed_kt = :kt
  AND atcf_id IN :ids
"""
OBSV_Q = """
SELECT atcf_id, valid_time, pop_exposed
FROM storms.nhc_tracks_obsv_exposure
WHERE iso3 = 'HTI' AND admin_level = 0 AND wind_speed_kt = :kt
  AND atcf_id IN :ids
"""

PATHWAYS = {
    "fcast_exp": "Prévision — exposition à 64 nœuds",
    "fcast_rain": "Prévision — précipitations",
    "orange": "Alerte orange DGPC (départements)",
    "obsv_exp": "Observation — exposition à 64 nœuds",
    "obsv_rain": "Observation — précipitations",
}


def _gefs_daily():
    g = pd.concat(
        [
            chirps_gefs.load_chirps_gefs_mean_daily(),
            chirps_gefs.load_recent_chirps_gefs_mean_daily(),
        ]
    ).sort_values(["issue_date", "valid_date"])
    g["roll2"] = g.groupby("issue_date")["mean"].transform(
        lambda s: s.rolling(2, min_periods=1).sum()
    )
    g["issue_time"] = g["issue_date"] + pd.Timedelta(
        hours=dc.GEFS_ISSUE_HOUR_UTC
    )
    return g


def _window(s):
    first = pd.Timestamp(s.first_near).normalize()
    last = pd.Timestamp(s.last_near).normalize()
    pad = pd.Timedelta(days=dc.RAIN_WINDOW_PAD_DAYS)
    return first - pad, last + pad


def advisories(storms, gefs, rain_long, cutoff_h=LT_CUTOFF_HRS):
    """One row per NHC advisory, evaluated the way the monitoring does.

    Mirrors ``monitoring_utils.process_fcast_advisory``: interpolate the
    forecast track, take the forecast closest pass and its lead time,
    attribute rain to the dates the track is within D_THRESH (within the
    mobilisation lead cap), and read the CHIRPS-GEFS issuance most recent
    before the advisory. The same GEFS issuance and date window feed the
    DGPC department rain reading, so both systems see the same forecast.
    """
    adm0 = codab.load_codab_from_blob().to_crs(3857)
    ids = tuple(storms.atcf_id)
    with sdb.get_engine().connect() as conn:
        tracks = gpd.read_postgis(
            text(TRACK_Q), conn, params={"ids": ids}, geom_col="geometry"
        )
    lt_cap = TRIGGERS["mobilisation"]["lt_max_hrs"]
    thr_mm = dc.ORANGE_RAIN["threshold_mm"]

    rows = []
    for (aid, issued), grp in tracks.groupby(["atcf_id", "issued_time"]):
        gi = _interp_track(grp, "valid_time", adm0)
        gi["leadtime"] = gi["valid_time"] - issued
        closest = gi.loc[gi["distance"].idxmin()]
        ttc_h = closest["leadtime"].total_seconds() / 3600
        near = gi[
            (gi["distance"] < D_THRESH)
            & (gi["leadtime"] <= pd.Timedelta(hours=lt_cap))
        ]
        rec = {
            "atcf_id": aid,
            "issued_time": issued,
            "time_to_closest_h": ttc_h,
            "past_cutoff": ttc_h < cutoff_h,
            "min_dist_km": float(gi["distance"].min()),
            "fcast_rain_mm": np.nan,
            "gefs_issue_date": pd.NaT,
            "rain_window_start": pd.NaT,
            "rain_window_end": pd.NaT,
            "orange_rain_depts": "",
        }
        if len(near):
            d0 = near["valid_time"].min().normalize()
            d1 = (near["valid_time"].max() + pd.Timedelta(days=1)).normalize()
            # The historical backtest paired each advisory with the GEFS
            # issuance of the same date (or earlier); the live monitoring
            # pairs on the 08:50 UTC issue time. The thresholds were
            # calibrated on the former, so it is used here.
            rec["rain_window_start"], rec["rain_window_end"] = d0, d1
            gdate = gefs.loc[
                gefs["issue_date"] <= issued.normalize(), "issue_date"
            ].max()
            if pd.notna(gdate):
                gsel = gefs[
                    (gefs["issue_date"] == gdate)
                    & (gefs["valid_date"] >= d0)
                    & (gefs["valid_date"] <= d1)
                ]
                rec["fcast_rain_mm"] = (
                    float(gsel["roll2"].max()) if len(gsel) else np.nan
                )
                rec["gefs_issue_date"] = gdate
                rsel = rain_long[
                    (rain_long["atcf_id"] == aid)
                    & (rain_long["issue_date"] == gdate)
                    & (rain_long["valid_date"] >= d0)
                    & (rain_long["valid_date"] <= d1)
                ]
                rec["orange_rain_depts"] = ",".join(
                    sorted(
                        rsel.loc[rsel["pix_max_mm"] >= thr_mm, "dept"].unique()
                    )
                )
        rows.append(rec)
    return pd.DataFrame(rows)


# Values the deck's activation record carries for Melissa 2025, which the
# historical monitors archive (2000-2023) does not cover.
MELISSA = {"atcf_id": "AL132025", "fcast_rain_mm": 82.5, "obsv_rain_mm": 80.0}


def calibration_monitors():
    """The backtest monitors the 2026 thresholds were calibrated on."""
    from src.datasources import nhc

    m = nhc.load_hist_fcast_monitors(lt_cutoff_hrs=48)
    m["atcf_id"] = m["atcf_id"].str.upper()
    return m


def apply_calibration(adv, monitors):
    """Override cutoff flags and forecast rain with the backtest's values
    where it has the advisory, so the framework pathways reproduce the
    record the thresholds were calibrated on."""
    # The deck's record reads forecast rain at the Action cap (72 h);
    # at the 120 h Mobilisation cap Ike 2008 would also reach 68 mm.
    old = monitors[monitors["lt_name"] == "action"][
        ["atcf_id", "issue_time", "past_cutoff", "roll2_rain_dist"]
    ].rename(
        columns={
            "issue_time": "issued_time",
            "past_cutoff": "past_cutoff_cal",
            "roll2_rain_dist": "fcast_rain_cal",
        }
    )
    out = adv.merge(old, on=["atcf_id", "issued_time"], how="left")
    has = out["past_cutoff_cal"].notna()
    out.loc[has, "past_cutoff"] = out.loc[has, "past_cutoff_cal"].astype(bool)
    out.loc[has, "fcast_rain_mm"] = out.loc[has, "fcast_rain_cal"]
    out["calibrated"] = has
    return out.drop(columns=["past_cutoff_cal", "fcast_rain_cal"])


def exposure_by_advisory(adv):
    """Forecast 64 kt exposure per advisory: future-only cone plus the
    observed swath accumulated by the issuance (fcastonly + obsv)."""
    e = sdb.get_engine()
    ids = tuple(adv.atcf_id.unique())
    f = pd.read_sql(
        text(EXP_Q), e, params={"kt": EXPOSURE_WIND_KT, "ids": ids}
    )
    o = pd.read_sql(
        text(OBSV_Q), e, params={"kt": EXPOSURE_WIND_KT, "ids": ids}
    )
    out = adv[["atcf_id", "issued_time"]].merge(
        f.rename(columns={"pop_exposed": "fcastonly_64"}),
        on=["atcf_id", "issued_time"],
        how="left",
    )
    out["fcastonly_64"] = out["fcastonly_64"].fillna(0.0)
    swath = []
    for _, r in out.iterrows():
        oo = o[(o.atcf_id == r.atcf_id) & (o.valid_time <= r.issued_time)]
        swath.append(float(oo["pop_exposed"].max()) if len(oo) else 0.0)
    out["obsv_swath_64"] = swath
    out["fcast_exp_64"] = out["fcastonly_64"] + out["obsv_swath_64"]
    obsv_max = o.groupby("atcf_id")["pop_exposed"].max().rename("obsv_exp_64")
    return out, obsv_max


def orange_wind_by_advisory(fcast, variant=dc.ORANGE_WIND_VARIANT):
    """Departments whose forecast wind reaches 100 km/h, per advisory."""
    w = wind_forecast_by_dept(fcast, variant)
    w = w[w.wind_kt >= dc.ORANGE_WIND_KT]
    return (
        w.groupby(["atcf_id", "issued_time"])["dept"]
        .apply(lambda s: ",".join(sorted(s.unique())))
        .rename("orange_wind_depts")
        .reset_index()
    )


def _union_depts(series):
    out = set()
    for s in series.dropna():
        if s:
            out |= set(s.split(","))
    return out


def per_storm(storms, adv, exp_adv, obsv_max, wind_adv, rain_long, monitors):
    """Reduce the pre-cutoff advisories to one row per storm."""
    obs_cal = (
        monitors[monitors["lt_name"] == "obsv"]
        .groupby("atcf_id")["roll2_rain_dist"]
        .max()
    )
    a = adv.merge(exp_adv, on=["atcf_id", "issued_time"], how="left").merge(
        wind_adv, on=["atcf_id", "issued_time"], how="left"
    )
    im = imerg.load_imerg_from_postgres().sort_values("date")
    im["roll2"] = im["mean"].rolling(2, min_periods=1).sum()
    has_rain = set(rain_long.atcf_id)

    rows = []
    for _, s in storms.iterrows():
        aid = s.atcf_id
        pre = a[(a.atcf_id == aid) & ~a.past_cutoff]
        lo, hi = _window(s)
        obs = im[(im.date >= lo) & (im.date <= hi)]
        wd = _union_depts(pre["orange_wind_depts"])
        rd = _union_depts(pre["orange_rain_depts"])
        # The same maxima over *every* advisory, cutoff ignored: what the
        # forecasts eventually said, even when too late to act on.
        every = a[a.atcf_id == aid]
        wd_all = _union_depts(every["orange_wind_depts"])
        rd_all = _union_depts(every["orange_rain_depts"])
        calibrated = bool(a.loc[a.atcf_id == aid, "calibrated"].any())
        if aid in obs_cal.index:
            obsv_rain = float(obs_cal[aid])
        else:
            obsv_rain = float(obs["roll2"].max()) if len(obs) else np.nan
        fcast_rain = (
            float(pre["fcast_rain_mm"].max())
            if pre["fcast_rain_mm"].notna().any()
            else np.nan
        )
        if aid == MELISSA["atcf_id"]:
            fcast_rain, obsv_rain = (
                MELISSA["fcast_rain_mm"],
                MELISSA["obsv_rain_mm"],
            )
            calibrated = True
        rows.append(
            {
                "atcf_id": aid,
                "n_advisories": int((a.atcf_id == aid).sum()),
                "n_pre_cutoff": len(pre),
                "fcast_exp_64": (
                    float(pre["fcast_exp_64"].max()) if len(pre) else 0.0
                ),
                "fcast_rain_mm": fcast_rain,
                "obsv_exp_64": float(obsv_max.get(aid, 0.0)),
                "obsv_rain_mm": obsv_rain,
                "calibrated": calibrated,
                "fcast_exp_64_all": (
                    float(every["fcast_exp_64"].max()) if len(every) else 0.0
                ),
                "fcast_rain_mm_all": (
                    float(every["fcast_rain_mm"].max())
                    if every["fcast_rain_mm"].notna().any()
                    else np.nan
                ),
                "n_orange_all": len(wd_all | rd_all),
                "n_orange_wind": len(wd),
                "n_orange_rain": len(rd) if aid in has_rain else np.nan,
                "n_orange": len(wd | rd),
                "orange_depts": ", ".join(sorted(wd | rd)),
                "orange_rain_known": aid in has_rain,
            }
        )
    return pd.DataFrame(rows)


def build(cutoff_h=LT_CUTOFF_HRS):
    storms = blob.load_parquet_from_blob(f"{PREFIX}/storm_set.parquet")
    fcast = blob.load_parquet_from_blob(
        f"{PREFIX}/fcast_wind_by_issuance.parquet"
    )
    rain_long = blob.load_parquet_from_blob(
        f"{PREFIX}/dept_rain_fcast.parquet"
    )
    gefs = _gefs_daily()
    monitors = calibration_monitors()
    adv = apply_calibration(
        advisories(storms, gefs, rain_long, cutoff_h), monitors
    )
    exp_adv, obsv_max = exposure_by_advisory(adv)
    wind_adv = orange_wind_by_advisory(fcast)
    df = storms[
        ["atcf_id", "name", "season", "closest_time", "min_dist_km"]
    ].merge(
        per_storm(
            storms, adv, exp_adv, obsv_max, wind_adv, rain_long, monitors
        ),
        on="atcf_id",
    )
    return df.sort_values("season").reset_index(drop=True), adv


def flags(df, rain_fcast_mm=68, rain_obsv_mm=57, orange_n=None, exp_pop=0):
    """Pathway flags for one threshold set; ``orange_n=None`` = off."""
    out = pd.DataFrame({"atcf_id": df.atcf_id, "season": df.season})
    out["fcast_exp"] = df.fcast_exp_64 > exp_pop
    out["fcast_rain"] = df.fcast_rain_mm >= rain_fcast_mm
    out["obsv_exp"] = df.obsv_exp_64 > exp_pop
    out["obsv_rain"] = df.obsv_rain_mm >= rain_obsv_mm
    out["orange"] = (
        (df.n_orange >= orange_n)
        if orange_n is not None
        else pd.Series(False, index=df.index)
    )
    out["any"] = out[list(PATHWAYS)].fillna(False).astype(bool).any(axis=1)
    return out


def rp_table(fl):
    rows = []
    for k, lab in list(PATHWAYS.items()) + [("any", "Ensemble")]:
        hit = fl[fl[k].fillna(False).astype(bool)]
        ny = hit.season.nunique()
        rows.append(
            {
                "pathway": k,
                "label": lab,
                "n_storms": len(hit),
                "n_years": ny,
                "rp_years": (N_YEARS + 1) / ny if ny else np.inf,
            }
        )
    return pd.DataFrame(rows)


def search(df, target_storms, orange_ns=(4, 5, 6, 7, 8)):
    """Threshold sets whose combined record has ``target_storms``
    activations, with the orange pathway at each N."""
    rows = []
    for n in orange_ns:
        for rf in range(60, 131, 2):
            for ro in range(50, 111, 2):
                fl = flags(df, rf, ro, n)
                if int(fl["any"].sum()) == target_storms:
                    rp = rp_table(fl).set_index("pathway")
                    rows.append(
                        {
                            "orange_n": n,
                            "rain_fcast_mm": rf,
                            "rain_obsv_mm": ro,
                            "n_storms": int(fl["any"].sum()),
                            "n_years": int(rp.loc["any", "n_years"]),
                            "storms": ", ".join(
                                df.loc[fl["any"], "name"].str.title()
                            ),
                        }
                    )
    return pd.DataFrame(rows)


def dept_table(storms, adv, fcast, rain_long, variant=dc.ORANGE_WIND_VARIANT):
    """One row per (storm, department) on pre-cutoff advisories only.

    Same columns as ``dept_forecast.department_verdicts`` so the page
    helpers (``storm_counts``, ``department_frequency``) apply unchanged,
    but every value comes from an advisory issued before the cutoff and
    from the CHIRPS-GEFS issuance paired with that advisory.
    """
    thr_mm = dc.ORANGE_RAIN["threshold_mm"]
    pre = adv[~adv.past_cutoff][
        ["atcf_id", "issued_time", "time_to_closest_h", "gefs_issue_date"]
    ]

    # Wind: per-department max over pre-cutoff advisories.
    w = wind_forecast_by_dept(fcast, variant).merge(
        pre[["atcf_id", "issued_time", "time_to_closest_h"]].rename(
            columns={"time_to_closest_h": "lead_h"}
        ),
        on=["atcf_id", "issued_time"],
        how="inner",
    )
    gw = w.groupby(["atcf_id", "dept"])
    wind = pd.DataFrame(
        {
            "wind_max_kt": gw["wind_kt"].max(),
            "wind_lead_h": gw.apply(
                lambda d: d.loc[d.wind_kt >= dc.ORANGE_WIND_KT, "lead_h"].max()
            ),
            "n_wind_fcasts": gw.size(),
        }
    )

    # Rain: the GEFS issuance paired with each pre-cutoff advisory, over
    # that advisory's attribution window, with the advisory's lead time.
    pairs = (
        adv[~adv.past_cutoff]
        .dropna(subset=["gefs_issue_date"])[
            [
                "atcf_id",
                "gefs_issue_date",
                "rain_window_start",
                "rain_window_end",
                "time_to_closest_h",
            ]
        ]
        .rename(
            columns={
                "gefs_issue_date": "issue_date",
                "time_to_closest_h": "lead_h",
            }
        )
    )
    r = rain_long.merge(pairs, on=["atcf_id", "issue_date"], how="inner")
    r = r[
        (r.valid_date >= r.rain_window_start)
        & (r.valid_date <= r.rain_window_end)
    ]
    gr = r.groupby(["atcf_id", "dept"])
    rain = pd.DataFrame(
        {
            "rain_mean_max_mm": gr["mean_mm"].max(),
            "rain_pix_max_mm": gr["pix_max_mm"].max(),
            "rain_mean_lead_h": gr.apply(
                lambda d: d.loc[d.mean_mm >= thr_mm, "lead_h"].max()
            ),
            "rain_pix_lead_h": gr.apply(
                lambda d: d.loc[d.pix_max_mm >= thr_mm, "lead_h"].max()
            ),
            "n_rain_fcasts": gr.size(),
        }
    )

    dept_names = sorted(set(rain_long.dept) | set(w.dept))
    idx = pd.MultiIndex.from_product(
        [storms.atcf_id.tolist(), dept_names], names=["atcf_id", "dept"]
    )
    out = pd.DataFrame(index=idx).join(wind).join(rain).reset_index()
    out["n_wind_fcasts"] = out["n_wind_fcasts"].fillna(0).astype(int)
    out["n_rain_fcasts"] = out["n_rain_fcasts"].fillna(0).astype(int)
    out["wind_max_kmh"] = out["wind_max_kt"] * 1.852

    # A storm with no advisory before the cutoff could not have been put
    # in orange in time: that is a 0, not an unknown. The only unknowns are
    # the storms with no CHIRPS-GEFS archive at all (the 2020 gap).
    known_w = pd.Series(True, index=out.index)
    known_r = out.atcf_id.isin(set(rain_long.atcf_id))

    def _flag(series, thr, known):
        return (series.fillna(-1) >= thr).where(known)

    out["orange_wind"] = _flag(out["wind_max_kt"], dc.ORANGE_WIND_KT, known_w)
    out["orange_rain_mean"] = _flag(out["rain_mean_max_mm"], thr_mm, known_r)
    out["orange_rain_pix"] = _flag(out["rain_pix_max_mm"], thr_mm, known_r)
    for tag in ("mean", "pix"):
        wv, rv = out["orange_wind"], out[f"orange_rain_{tag}"]
        met = wv.fillna(False).astype(bool) | rv.fillna(False).astype(bool)
        unknown = ~met & (wv.isna() | rv.isna())
        out[f"orange_any_{tag}"] = met.where(~unknown)
        out[f"any_{tag}_lead_h"] = pd.concat(
            [out["wind_lead_h"], out[f"rain_{tag}_lead_h"]], axis=1
        ).max(axis=1)
    return out.merge(
        storms[["atcf_id", "name", "season", "closest_time"]],
        on="atcf_id",
        how="left",
    )
