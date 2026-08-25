"""Turn raw wind/rain output into per-storm verdicts and return periods."""

import numpy as np
import pandas as pd

from src.dgpc import constants as dc

WIND_VARIANTS = tuple(dc.WIND_VARIANTS)


def summarise_wind_forecast(fcast: pd.DataFrame) -> pd.DataFrame:
    """Per-storm maxima across issuances, plus lead time at first exceedance.

    ``lead_h_orange`` / ``lead_h_red`` are the hours between the earliest
    issuance meeting the level and the storm's forecast closest approach —
    i.e. how much warning the level would have given.
    """
    rows = []
    for atcf_id, g in fcast.groupby("atcf_id"):
        g = g.sort_values("issued_time")
        rec = {"atcf_id": atcf_id, "n_issuances": len(g)}
        for v in WIND_VARIANTS:
            col = f"{v}_max_kt"
            if col not in g:
                continue
            rec[f"{v}_max_kt"] = float(g[col].max())
            rec[f"{v}_max_kmh"] = float(g[col].max()) * 1.852
            rec[f"{v}_orange"] = bool((g[col] >= dc.ORANGE_WIND_KT).any())
            rec[f"{v}_red"] = bool((g[col] >= dc.RED_WIND_KT).any())
            rec[f"{v}_pop_orange"] = float(g[f"{v}_pop_orange"].max())
            rec[f"{v}_pop_red"] = float(g[f"{v}_pop_red"].max())
            for lvl, thr in (
                ("orange", dc.ORANGE_WIND_KT),
                ("red", dc.RED_WIND_KT),
            ):
                hit = g[g[col] >= thr]
                rec[f"{v}_lead_h_{lvl}"] = (
                    float(hit.iloc[0]["leadtime_to_closest_h"])
                    if len(hit)
                    else np.nan
                )
                rec[f"{v}_first_issue_{lvl}"] = (
                    hit.iloc[0]["issued_time"] if len(hit) else pd.NaT
                )
        rec["max_fcast_vmax_kt"] = float(g["max_fcast_vmax_kt"].max())
        rows.append(rec)
    return pd.DataFrame(rows)


def summarise_wind_observed(obsv: pd.DataFrame) -> pd.DataFrame:
    """Per-storm verdicts from the best track."""
    out = obsv[["atcf_id", "obsv_vmax_kt"]].copy()
    for v in WIND_VARIANTS:
        col = f"{v}_max_kt"
        if col not in obsv:
            continue
        out[f"obsv_{v}_max_kt"] = obsv[col]
        out[f"obsv_{v}_max_kmh"] = obsv[col] * 1.852
        out[f"obsv_{v}_orange"] = obsv[col] >= dc.ORANGE_WIND_KT
        out[f"obsv_{v}_red"] = obsv[col] >= dc.RED_WIND_KT
        out[f"obsv_{v}_pop_orange"] = obsv[f"{v}_pop_orange"]
        out[f"obsv_{v}_pop_red"] = obsv[f"{v}_pop_red"]
    return out


def return_period(n_years: int, activation_years: int) -> float:
    """Weibull plotting position: RP = (n + 1) / activations."""
    if activation_years <= 0:
        return np.inf
    return (n_years + 1) / activation_years


def rp_table(
    per_storm: pd.DataFrame, flag_cols, season_col="season"
) -> pd.DataFrame:
    """Return period per criterion, counting *years* with >=1 activation."""
    n_years = dc.SEASON_END - dc.SEASON_START + 1
    rows = []
    for col, label in flag_cols.items():
        if col not in per_storm:
            continue
        hits = per_storm[per_storm[col].fillna(False).astype(bool)]
        yrs = sorted(hits[season_col].unique())
        rows.append(
            {
                "criterion": label,
                "column": col,
                "n_storms": len(hits),
                "n_years": len(yrs),
                "years": ", ".join(str(y) for y in yrs),
                "rp_years": return_period(n_years, len(yrs)),
                "annual_prob": (len(yrs) / n_years) if n_years else np.nan,
            }
        )
    return pd.DataFrame(rows)


def overall_rp(per_storm: pd.DataFrame, cols, season_col="season") -> dict:
    """Years with at least one of ``cols`` met."""
    n_years = dc.SEASON_END - dc.SEASON_START + 1
    present = [c for c in cols if c in per_storm]
    if not present:
        return {"n_years": 0, "rp_years": np.inf, "years": ""}
    any_hit = per_storm[present].fillna(False).astype(bool).any(axis=1)
    yrs = sorted(per_storm.loc[any_hit, season_col].unique())
    return {
        "n_years": len(yrs),
        "years": ", ".join(str(y) for y in yrs),
        "rp_years": return_period(n_years, len(yrs)),
        "annual_prob": len(yrs) / n_years if n_years else np.nan,
    }
