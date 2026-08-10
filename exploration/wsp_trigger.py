import marimo

__generated_with = "0.23.6"
app = marimo.App(width="full")


@app.cell
def imports():
    import geopandas as gpd
    import marimo as mo
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import ocha_stratus as stratus
    import pandas as pd
    from sqlalchemy import text

    from src.datasources import nhc
    from src.datasources.codab import load_codab_from_blob
    from src.utils.blob import PROJECT_PREFIX

    return (
        PROJECT_PREFIX,
        gpd,
        load_codab_from_blob,
        mo,
        mpatches,
        nhc,
        pd,
        plt,
        stratus,
        text,
    )


@app.cell
def doc_activation_summary(mo):
    mo.md(
        r"""
    # Activation summary

    Summary of the **selected trigger** and its historical performance. This is the
    top-level results view; the full optimization that produces it lives further
    down (the *Hurricane Warning OR trigger* section — see `rain_trigger_opt_hur`).

    ## Selected trigger (Hurricane Warning OR, 64 kt ★)

    A storm **activates** if *any* of these fire:

    **Forecast**
    - **Exposure** — fcastonly + cumulative-observed population exposed to ≥ 64 kt
      winds > 0 (the optimised exposure threshold is the smallest non-zero historical
      value, i.e. operationally ">0 people exposed"). Source: `df_total_exp`, built
      from `nhc_tracks_fcastonly_exposure` + `nhc_tracks_obsv_exposure`.
    - **Rainfall** — forecast 2-day rolling rainfall ≥ the optimised threshold,
      from NHC action-leadtime monitors.
    - **DGPC Rouge + Hur. Warning** — storm carried an NHC Hurricane Warning for
      Haiti. These flags are **hardcoded** in the `load_nhc_alerts` cell
      (`df_nhc_alerts`), keyed on (name, season), not read from a live feed.

    **Observed**
    - **Exposure** — observed-only ≥ 64 kt exposure ≥ the same exposure threshold.
    - **Rainfall** — observed 2-day rolling rainfall ≥ the optimised threshold.

    The exposure/rainfall thresholds and the n = 12 target are set by the
    optimization described in the *Trigger optimization* doc cell below.

    ## Metric definitions

    All return periods are computed **per season** over the historical record
    (2002–present); `total_seasons` = span of seasons in the data.

    | Metric | Formula |
    |---|---|
    | **Overall return period** | `total_seasons / seasons_with_activation` |
    | **Overall probability of activation** | `seasons_with_activation / total_seasons` (= 1 / overall RP) |
    | **Average total spending per year** | `n_activations × $4M / total_seasons` |
    | **Effective RP** (avg period for a full budget spend) | `total_seasons / n_activations` — counts each activation, so seasons with multiple activations shorten it relative to the overall RP |
    | **Probability of total budget spend** | `avg_spend_per_year / $4M` = `n_activations / total_seasons` |
    | **Average spending per year with activation** | `n_activations × $4M / seasons_with_activation` |

    Each activation costs a flat **$4M** (`_BUDGET`).

    **Per-indicator return periods** use the same per-season definition:
    `total_seasons / (seasons in which that indicator fired ≥ once)`. The "overall"
    forecast / observed rows are the union of their components; "exposure OR
    rainfall (no DGPC)" drops the Hurricane Warning condition so its marginal
    contribution is visible. Note these section-level RPs are unions *within* a
    section and do **not** equal the trigger-wide Overall RP (which unions every
    forecast + observed + hur-warning condition).
    """
    )
    return


@app.cell
def activation_summary(df_rain_opt_hur, mo, pd, rain_opt_thresh_hur):
    mo.stop(not rain_opt_thresh_hur)

    _BUDGET = 4_000_000
    _WKT = 64
    _et = rain_opt_thresh_hur[_WKT]["exp_thresh"]

    # ── Derived flags (on full frame) ─────────────────────────────────────
    _full = df_rain_opt_hur.copy()
    _full["_obs_exp_flag"] = _full["exp_64"].fillna(0) >= _et
    _full["_fcast_no_dgpc"] = (
        _full[f"_exp_flag_{_WKT}"] | _full[f"_fcast_flag_{_WKT}"]
    )
    _full["_overall_fcast"] = _full["_fcast_no_dgpc"] | _full["hur_warning"]
    _full["_overall_obs"] = (
        _full["_obs_exp_flag"] | _full[f"_rain_flag_{_WKT}"]
    )

    # Filter: triggered OR has impact OR has CERF
    _show = (
        _full[f"combined_{_WKT}"]
        | (_full["Total Affected"].fillna(0) > 0)
        | _full["has_cerf"]
    )
    _df = (
        _full[_show]
        .sort_values("Total Affected", ascending=False, na_position="last")
        .reset_index(drop=True)
    )

    # ── Metrics inputs ────────────────────────────────────────────────────
    # Use full frame for season counts (not the filtered display slice)
    _total_seasons = int(
        _full["season"].dropna().max() - _full["season"].dropna().min() + 1
    )
    _n_act = int(_full[f"combined_{_WKT}"].sum())
    _seasons_with_act = int(
        _full[_full[f"combined_{_WKT}"]]["season"]
        .dropna()
        .astype(int)
        .nunique()
    )
    _overall_rp = _total_seasons / _seasons_with_act
    _prob_act = _seasons_with_act / _total_seasons
    _avg_spend = _n_act * _BUDGET / _total_seasons
    _eff_rp = _total_seasons / _n_act
    _prob_budget = _n_act / _total_seasons
    _avg_spend_with_act = _n_act * _BUDGET / _seasons_with_act

    def _fmt_usd(x):
        return f"${x / 1_000_000:.2f}M"

    # ── Per-indicator return periods ──────────────────────────────────────
    # RP = total seasons / number of seasons with at least one firing
    def _seasons_with(_flag_col):
        return int(
            _full[_full[_flag_col]]["season"].dropna().astype(int).nunique()
        )

    _indicators = [
        ("Forecast — exposure", f"_exp_flag_{_WKT}"),
        ("Forecast — rainfall", f"_fcast_flag_{_WKT}"),
        ("Forecast — DGPC Rouge + Hur. Warning", "hur_warning"),
        ("Forecast — exposure OR rainfall (no DGPC)", "_fcast_no_dgpc"),
        ("Forecast — overall", "_overall_fcast"),
        ("Observed — exposure", "_obs_exp_flag"),
        ("Observed — rainfall", f"_rain_flag_{_WKT}"),
        ("Observed — overall", "_overall_obs"),
    ]
    _rp_rows = []
    for _label, _col in _indicators:
        _ns = _seasons_with(_col)
        _rp_str = f"{_total_seasons / _ns:.1f} yrs" if _ns else "—"
        _prob_str = f"{_ns / _total_seasons:.1%}" if _ns else "—"
        _calc = (
            f"{_total_seasons} seasons / {_ns} seasons triggered"
            if _ns
            else "never triggered"
        )
        _rp_rows.append(f"| {_label} | {_rp_str} | {_prob_str} | {_calc} |")
    _rp_md = (
        "| Indicator | Return period | Probability (1/RP) | Calculation |\n"
        "|:---|---:|---:|:---|\n" + "\n".join(_rp_rows)
    )

    _metrics_md = f"""
    | Metric | Value | Calculation |
    |:---|---:|:---|
    | Overall return period | {_overall_rp:.1f} yrs | {_total_seasons} seasons / {_seasons_with_act} seasons with activation |
    | Overall probability of activation | {_prob_act:.1%} | {_seasons_with_act} / {_total_seasons} seasons |
    | Average total spending per year | {_fmt_usd(_avg_spend)} | {_n_act} activations × $4M / {_total_seasons} seasons |
    | Avg period for total budget spend / Effective RP | {_eff_rp:.1f} yrs | {_total_seasons} seasons / {_n_act} activations |
    | Probability of total budget spend | {_prob_budget:.1%} | {_n_act} / {_total_seasons} seasons |
    | Average total spending per year with activation | {_fmt_usd(_avg_spend_with_act)} | {_n_act} activations × $4M / {_seasons_with_act} seasons with activation |
    """

    # ── Table HTML ────────────────────────────────────────────────────────
    def _cerf_fmt(row):
        if pd.notna(row.get("Amount in US$")) and row["Amount in US$"] > 0:
            return f"${row['Amount in US$']:,.0f}"
        if pd.notna(row.get("season")) and int(row["season"]) == 2008:
            return "combined"
        if pd.notna(row.get("season")) and int(row["season"]) >= 2006:
            return "—"
        return "pre-"

    def _fmt_exp(x):
        return f"{int(x):,}" if pd.notna(x) and x > 0 else "—"

    def _fmt_rain(x):
        return f"{x:.0f} mm" if pd.notna(x) and x > 0 else "—"

    def _tick(val):
        return "✓" if val else "—"

    def _fmt_aff(x):
        return f"{int(x):,}" if pd.notna(x) and x > 0 else "—"

    _max_aff = _df["Total Affected"].fillna(0).max()

    _ORG = "background-color:#f57c00;color:white;font-weight:bold"
    _RED_CELL = "background-color:#c62828;color:white;font-weight:bold"

    def _td(content, flag=False, cerf_cell=False, cls="", bar_val=None):
        if flag:
            _s = _ORG
        elif cerf_cell:
            _s = _RED_CELL
        elif (
            bar_val is not None
            and pd.notna(bar_val)
            and bar_val > 0
            and _max_aff > 0
        ):
            _pct = min(100, bar_val / _max_aff * 100)
            _s = f"background-image:linear-gradient(to right,#b39ddb {_pct:.0f}%,transparent {_pct:.0f}%)"
        else:
            _s = ""
        _ca = f' class="{cls}"' if cls else ""
        _sa = f' style="{_s}"' if _s else ""
        return f"<td{_ca}{_sa}>{content}</td>"

    _rows_html = []
    for _, _r in _df.iterrows():
        _cells = [
            f'<td class="lc">{_r["Storm"]}</td>',
            _td(
                _fmt_exp(_r["total_exp_64"]),
                flag=bool(_r[f"_exp_flag_{_WKT}"]),
                cls="bl-f",
            ),
            _td(
                _fmt_rain(_r["max_fcast_rain"]),
                flag=bool(_r[f"_fcast_flag_{_WKT}"]),
            ),
            _td(
                _tick(_r["hur_warning"]),
                flag=bool(_r["hur_warning"]),
                cls="br-f",
            ),
            _td(
                _fmt_exp(_r["exp_64"]),
                flag=bool(_r["_obs_exp_flag"]),
                cls="bl-o",
            ),
            _td(
                _fmt_rain(_r["max_obs_rain"]),
                flag=bool(_r[f"_rain_flag_{_WKT}"]),
                cls="br-o",
            ),
            _td(
                _tick(_r[f"combined_{_WKT}"]),
                flag=bool(_r[f"combined_{_WKT}"]),
            ),
            _td(
                _cerf_disp := _cerf_fmt(_r),
                cerf_cell=_cerf_disp not in ("—", "pre-"),
            ),
            _td(_fmt_aff(_r["Total Affected"]), bar_val=_r["Total Affected"]),
        ]
        _rows_html.append(f"<tr>{''.join(_cells)}</tr>")

    _css = """<style>
      .act-tbl{border-collapse:collapse;font-size:13px;font-family:sans-serif}
      .act-tbl th,.act-tbl td{padding:5px 10px;text-align:center;border-bottom:1px solid #e0e0e0}
      .act-tbl th{background:#f5f5f5;border-bottom:2px solid #ccc;white-space:nowrap}
      .act-tbl .lc{text-align:left}
      .act-tbl tbody tr:hover td{box-shadow:inset 0 0 0 9999px rgba(0,0,0,0.10);cursor:default}
      .act-tbl .fcast-grp{background:#dde8f8}
      .act-tbl .obs-grp{background:#ddf0e8}
      .act-tbl .bl-f{border-left:2px solid #7baed6}
      .act-tbl .br-f{border-right:2px solid #7baed6}
      .act-tbl .bl-o{border-left:2px solid #6bbf8e}
      .act-tbl .br-o{border-right:2px solid #6bbf8e}
    </style>"""

    _thead = (
        "<thead>"
        "<tr>"
        '<th rowspan="2" class="lc">Système</th>'
        '<th colspan="3" class="fcast-grp bl-f br-f" style="font-size:11px;letter-spacing:.05em;text-transform:uppercase;color:#444">Prévision</th>'
        '<th colspan="2" class="obs-grp bl-o br-o" style="font-size:11px;letter-spacing:.05em;text-transform:uppercase;color:#444">Observationnel</th>'
        '<th rowspan="2">Déclenchement<br>global</th>'
        '<th rowspan="2">CERF</th>'
        '<th rowspan="2">Pop. affectée<br>totale</th>'
        "</tr>"
        "<tr>"
        '<th class="fcast-grp bl-f">Exp. prév.</th>'
        '<th class="fcast-grp">Précip. prév.</th>'
        '<th class="fcast-grp br-f">DGPC Rouge +<br>Hur. Warning</th>'
        '<th class="obs-grp bl-o">Exp. obs.</th>'
        '<th class="obs-grp br-o">Précip. obs.</th>'
        "</tr>"
        "</thead>"
    )

    _table_html = (
        f"{_css}\n"
        f'<table class="act-tbl">\n'
        f"{_thead}\n"
        f"<tbody>{''.join(_rows_html)}</tbody>\n"
        f"</table>"
    )

    mo.output.replace(
        mo.vstack(
            [
                mo.md(
                    "## Résumé des activations — déclencheur OR alerte ouragan, 64 kt ★"
                ),
                mo.Html(_table_html),
                mo.md("### Return periods by indicator"),
                mo.md(_rp_md),
                mo.md("### Overall metrics"),
                mo.md(_metrics_md),
            ]
        )
    )
    return


@app.cell
def load_wind_exposure(pd, stratus, text):
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _df_raw = pd.read_sql(
            text(
                """
                SELECT atcf_id, valid_time, wind_speed_kt, pop_exposed
                FROM storms.nhc_tracks_obsv_exposure
                WHERE iso3 = 'HTI' AND admin_level = 0
            """
            ),
            _conn,
        )
        _df_sid = pd.read_sql(
            text("SELECT sid, atcf_id FROM storms.ibtracs_storms"),
            _conn,
        )
    _engine.dispose()

    _df_raw = _df_raw.merge(_df_sid, on="atcf_id", how="left")
    # Keep only the last valid_time per (sid, wind_speed_kt)
    _df_raw = _df_raw.sort_values("valid_time")
    _df_raw = (
        _df_raw.groupby(["sid", "wind_speed_kt"], sort=False)
        .last()
        .reset_index()
    )
    df_exp_raw = _df_raw[["sid", "wind_speed_kt", "pop_exposed"]].dropna(
        subset=["sid"]
    )
    df_exp_raw = df_exp_raw[df_exp_raw["sid"].str[:4].astype(int) >= 2002]
    return (df_exp_raw,)


@app.cell
def load_storm_meta(df_exp_raw, pd, stratus, text):
    _sids = df_exp_raw["sid"].unique().tolist()
    _placeholders = ", ".join(f"'{s}'" for s in _sids)
    _engine = stratus.get_engine(stage="prod")
    with _engine.connect() as _conn:
        _df_meta = pd.read_sql(
            text(
                "SELECT sid, season, name FROM storms.ibtracs_storms"
                f" WHERE sid IN ({_placeholders})"
            ),
            _conn,
        )
    _engine.dispose()
    df_exp = df_exp_raw.merge(_df_meta, on="sid", how="left")
    return (df_exp,)


@app.cell
def load_impact(PROJECT_PREFIX, pd, stratus):
    try:
        _blob = (
            f"{PROJECT_PREFIX}/processed/impact/emdat_cerf_upto2024.parquet"
        )
        _df_all = stratus.load_parquet_from_blob(_blob)
        _keep = [
            "sid",
            "Event Name",
            "Start Year",
            "Total Affected",
            "Amount in US$",
        ]
        df_impact = _df_all[
            [c for c in _keep if c in _df_all.columns]
        ].drop_duplicates("sid")
    except Exception:
        df_impact = pd.DataFrame(
            columns=["sid", "Total Affected", "Amount in US$"]
        )
    # Lili 2002 not in EM-DAT for Haiti
    _LILI_SID = "2002265N10315"
    if _LILI_SID not in df_impact["sid"].values:
        df_impact = pd.concat(
            [
                df_impact,
                pd.DataFrame([{"sid": _LILI_SID, "Total Affected": 250}]),
            ],
            ignore_index=True,
        )
    else:
        df_impact.loc[df_impact["sid"] == _LILI_SID, "Total Affected"] = 250
    return (df_impact,)


@app.cell
def load_old_trigger(pd):
    # CERF amounts: Matthew = $5M+$1.6M (Oct) + $3.5M (Dec 2016);
    # Sandy = $4M (Nov 2012); 2008 season treated as one combined CERF event
    # (no per-storm breakdown available); Melissa imputed.
    _HIST = [
        {
            "name": "LILI",
            "season": 2002,
            "mob_trig": True,
            "obsv_trig": True,
            "Total Affected": 250,
            "Amount in US$": None,
        },
        {
            "name": "MATTHEW",
            "season": 2016,
            "mob_trig": True,
            "obsv_trig": True,
            "Total Affected": 2_100_439,
            "Amount in US$": 10_100_000,
        },
        {
            "name": "JEANNE",
            "season": 2004,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 315_594,
            "Amount in US$": None,
        },
        {
            "name": "SANDY",
            "season": 2012,
            "mob_trig": False,
            "obsv_trig": True,
            "Total Affected": 201_850,
            "Amount in US$": 4_000_000,
        },
        {
            "name": "IKE",
            "season": 2008,
            "mob_trig": True,
            "obsv_trig": True,
            "Total Affected": 125_050,
            "Amount in US$": None,
        },
        {
            "name": "NOEL",
            "season": 2007,
            "mob_trig": False,
            "obsv_trig": True,
            "Total Affected": 108_763,
            "Amount in US$": None,
        },
        {
            "name": "GUSTAV",
            "season": 2008,
            "mob_trig": False,
            "obsv_trig": True,
            "Total Affected": 73_006,
            "Amount in US$": None,
        },
        {
            "name": "HANNA",
            "season": 2008,
            "mob_trig": False,
            "obsv_trig": True,
            "Total Affected": 48_000,
            "Amount in US$": None,
        },
        {
            "name": "LAURA",
            "season": 2020,
            "mob_trig": False,
            "obsv_trig": True,
            "Total Affected": 44_175,
            "Amount in US$": None,
        },
        {
            "name": "IRMA",
            "season": 2017,
            "mob_trig": True,
            "obsv_trig": False,
            "Total Affected": 40_092,
            "Amount in US$": None,
        },
        {
            "name": "DENNIS",
            "season": 2005,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 15_036,
            "Amount in US$": None,
        },
        {
            "name": "ERNESTO",
            "season": 2006,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 15_000,
            "Amount in US$": None,
        },
        {
            "name": "STAN",
            "season": 2005,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 10_000,
            "Amount in US$": None,
        },
        {
            "name": "ISAAC",
            "season": 2012,
            "mob_trig": True,
            "obsv_trig": True,
            "Total Affected": 8_007,
            "Amount in US$": None,
        },
        {
            "name": "IVAN",
            "season": 2004,
            "mob_trig": True,
            "obsv_trig": False,
            "Total Affected": 6_500,
            "Amount in US$": None,
        },
        {
            "name": "TOMAS",
            "season": 2010,
            "mob_trig": True,
            "obsv_trig": True,
            "Total Affected": 5_020,
            "Amount in US$": None,
        },
        {
            "name": "DEAN",
            "season": 2007,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 3_966,
            "Amount in US$": None,
        },
        {
            "name": "OLGA",
            "season": 2007,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 2_352,
            "Amount in US$": None,
        },
        {
            "name": "ALPHA",
            "season": 2005,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 2_192,
            "Amount in US$": None,
        },
        {
            "name": "ERIKA",
            "season": 2015,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 1_969,
            "Amount in US$": None,
        },
        {
            "name": "IRENE",
            "season": 2011,
            "mob_trig": True,
            "obsv_trig": False,
            "Total Affected": 1_544,
            "Amount in US$": None,
        },
        {
            "name": "EMILY",
            "season": 2011,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 1_500,
            "Amount in US$": None,
        },
        {
            "name": "EMILY",
            "season": 2005,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 750,
            "Amount in US$": None,
        },
        {
            "name": "LILI",
            "season": 2002,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 250,
            "Amount in US$": None,
        },
        {
            "name": "FAY",
            "season": 2008,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 220,
            "Amount in US$": None,
        },
        {
            "name": "ELSA",
            "season": 2021,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 3,
            "Amount in US$": None,
        },
        {
            "name": "DEBBY",
            "season": 2000,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "HELENE",
            "season": 2000,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "IRIS",
            "season": 2001,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "MINDY",
            "season": 2003,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "ODETTE",
            "season": 2003,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "BONNIE",
            "season": 2004,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "CHRIS",
            "season": 2006,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "HENRI",
            "season": 2009,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "BONNIE",
            "season": 2010,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "CHANTAL",
            "season": 2013,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "BERTHA",
            "season": 2014,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "CRISTOBAL",
            "season": 2014,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "MARIA",
            "season": 2017,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "BERYL",
            "season": 2018,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "ISAIAS",
            "season": 2020,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "FRED",
            "season": 2021,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "GRACE",
            "season": 2021,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "FIONA",
            "season": 2022,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        {
            "name": "FRANKLIN",
            "season": 2023,
            "mob_trig": False,
            "obsv_trig": False,
            "Total Affected": 0,
            "Amount in US$": None,
        },
        # Melissa 2025 — imputed; not yet in EM-DAT
        {
            "name": "MELISSA",
            "season": 2025,
            "mob_trig": True,
            "obsv_trig": True,
            "Total Affected": 2_300_021,
            "Amount in US$": 4_000_000,
        },
    ]
    df_old_trig = pd.DataFrame(_HIST)
    return (df_old_trig,)


@app.cell
def load_monitors(nhc, pd):
    _mon_all = nhc.load_hist_fcast_monitors(lt_cutoff_hrs=48)

    # Observed rainfall: lt_name="obsv" rows represent 0-day hindcast values;
    # use these regardless of past_cutoff (they're always past_cutoff=True)
    _obs = _mon_all[_mon_all["lt_name"] == "obsv"]
    df_obs_rain = (
        _obs.groupby("sid")["roll2_rain_dist"]
        .max()
        .reset_index()
        .rename(columns={"roll2_rain_dist": "max_obs_rain"})
    )
    _melissa_obs = pd.DataFrame(
        [{"sid": "2025291N11319", "max_obs_rain": 80.0}]
    )
    df_obs_rain = pd.concat([df_obs_rain, _melissa_obs], ignore_index=True)

    # Forecast data: exclude past_cutoff rows
    _mon = _mon_all[~_mon_all["past_cutoff"]]
    df_rain = (
        _mon.groupby(["sid", "lt_name"])["roll2_rain_dist"]
        .max()
        .reset_index()
        .rename(columns={"roll2_rain_dist": "max_rain"})
    )
    # Old action trigger: any single forecast row where BOTH wind_dist>=64 AND rain>=42
    _mon_action = _mon[_mon["lt_name"] == "action"].copy()
    _mon_action["_both"] = (_mon_action["wind_dist"] >= 64) & (
        _mon_action["roll2_rain_dist"] >= 42
    )
    df_action_trig = (
        _mon_action.groupby("sid")["_both"]
        .any()
        .rename("action_trig_old")
        .reset_index()
    )
    # Melissa 2025 not in historical monitors archive
    _melissa_rain = pd.DataFrame(
        [
            {"sid": "2025291N11319", "lt_name": lt, "max_rain": 82.5}
            for lt in ["action", "readiness"]
        ]
    )
    _melissa_action = pd.DataFrame(
        [{"sid": "2025291N11319", "action_trig_old": True}]
    )
    df_rain = pd.concat([df_rain, _melissa_rain], ignore_index=True)
    df_action_trig = pd.concat(
        [df_action_trig, _melissa_action], ignore_index=True
    )
    df_mon_all = _mon_all
    return df_action_trig, df_mon_all, df_obs_rain, df_rain


@app.cell
def load_total_exposure(df_exp, df_mon_all, pd, stratus, text):
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _df_fcast = pd.read_sql(
            text(
                """
                SELECT atcf_id, issued_time, wind_speed_kt,
                       pop_exposed AS fcast_exp
                FROM storms.nhc_tracks_fcastonly_exposure
                WHERE iso3 = 'HTI' AND admin_level = 0
            """
            ),
            _conn,
        )
        _df_obsv = pd.read_sql(
            text(
                """
                SELECT atcf_id, valid_time, wind_speed_kt,
                       pop_exposed AS obsv_exp
                FROM storms.nhc_tracks_obsv_exposure
                WHERE iso3 = 'HTI' AND admin_level = 0
            """
            ),
            _conn,
        )
        _df_sid = pd.read_sql(
            text("SELECT sid, atcf_id FROM storms.ibtracs_storms"),
            _conn,
        )
    _engine.dispose()

    _df_fcast = _df_fcast.merge(_df_sid, on="atcf_id", how="left").dropna(
        subset=["sid"]
    )
    _df_obsv = _df_obsv.merge(_df_sid, on="atcf_id", how="left").dropna(
        subset=["sid"]
    )

    # Apply 48h pre-cutoff filter to forecast issuances
    _pre_cutoff = (
        df_mon_all[~df_mon_all["past_cutoff"]][["sid", "issue_time"]]
        .drop_duplicates()
        .rename(columns={"issue_time": "issued_time"})
    )
    _df_fcast = _df_fcast.merge(
        _pre_cutoff, on=["sid", "issued_time"], how="inner"
    )

    # For each (sid, wind_speed_kt): max(fcast_t + cumulative_obsv_t)
    _results = []
    for (_sid_v, _wkt), _fg in _df_fcast.groupby(["sid", "wind_speed_kt"]):
        _og = _df_obsv[
            (_df_obsv["sid"] == _sid_v) & (_df_obsv["wind_speed_kt"] == _wkt)
        ]
        _fs = _fg.sort_values("issued_time")
        if _og.empty:
            _max_total = float(_fs["fcast_exp"].max())
        else:
            _os = _og.sort_values("valid_time").assign(
                _cm=lambda x: x["obsv_exp"].cummax()
            )
            _m = pd.merge_asof(
                _fs[["issued_time", "fcast_exp"]],
                _os[["valid_time", "_cm"]].rename(
                    columns={"valid_time": "issued_time"}
                ),
                on="issued_time",
                direction="backward",
            )
            _max_total = float((_m["fcast_exp"] + _m["_cm"].fillna(0)).max())
        _results.append(
            {
                "sid": _sid_v,
                "wind_speed_kt": _wkt,
                "max_total_exposure": _max_total,
            }
        )

    _df_nhc = (
        pd.DataFrame(_results)
        if _results
        else pd.DataFrame(
            columns=["sid", "wind_speed_kt", "max_total_exposure"]
        )
    )

    # Fallback: storms not in nhc_tracks use df_exp (obsv) as proxy
    _nhc_sids = set(_df_nhc["sid"].unique()) if not _df_nhc.empty else set()
    _df_fill = df_exp[~df_exp["sid"].isin(_nhc_sids)][
        ["sid", "wind_speed_kt", "pop_exposed"]
    ].rename(columns={"pop_exposed": "max_total_exposure"})
    df_total_exp = pd.concat(
        [_df_nhc, _df_fill] if not _df_nhc.empty else [_df_fill],
        ignore_index=True,
    )
    df_total_exp = df_total_exp[
        df_total_exp["sid"].str[:4].astype(int) >= 2002
    ]

    _melissa_sid = "2025291N11319"
    if _melissa_sid not in df_total_exp["sid"].values:
        _mel = df_exp[df_exp["sid"] == _melissa_sid][
            ["sid", "wind_speed_kt", "pop_exposed"]
        ].rename(columns={"pop_exposed": "max_total_exposure"})
        df_total_exp = pd.concat([df_total_exp, _mel], ignore_index=True)
    return (df_total_exp,)


@app.cell
def exposure_check(df_exp, mo, pd, stratus, text):
    # Observed exposure: last valid_time per storm (already in df_exp)
    _obsv = (
        df_exp.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="pop_exposed",
            aggfunc="first",
        )
        .rename(columns={34: "obsv_34", 50: "obsv_50", 64: "obsv_64"})
        .reset_index()
    )
    # Forecast exposure: max across ALL issuances (no cutoff) — for inspection only
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _df_fe = pd.read_sql(
            text(
                """
                SELECT e.atcf_id, e.wind_speed_kt, MAX(e.pop_exposed) AS pop_exposed
                FROM storms.nhc_tracks_fcastonly_exposure e
                WHERE e.iso3 = 'HTI' AND e.admin_level = 0
                GROUP BY e.atcf_id, e.wind_speed_kt
            """
            ),
            _conn,
        )
        _df_sid = pd.read_sql(
            text("SELECT sid, atcf_id FROM storms.ibtracs_storms"), _conn
        )
    _engine.dispose()
    _df_fe = _df_fe.merge(_df_sid, on="atcf_id", how="left").dropna(
        subset=["sid"]
    )
    _fcast = (
        _df_fe.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="pop_exposed",
            aggfunc="max",
        )
        .rename(columns={34: "fcast_34", 50: "fcast_50", 64: "fcast_64"})
        .reset_index()
    )
    _meta = df_exp[["sid", "season", "name"]].drop_duplicates("sid")
    _tbl = (
        _meta.merge(_obsv, on="sid", how="outer")
        .merge(_fcast, on="sid", how="outer")
        .sort_values("obsv_34", ascending=False, na_position="last")
        .reset_index(drop=True)
    )
    _tbl["Storm"] = _tbl.apply(
        lambda r: f"{str(r['name']).strip().title()} ({int(r['season'])})"
        if pd.notna(r["name"]) and pd.notna(r["season"])
        else r["sid"],
        axis=1,
    )
    for _c in [
        "obsv_34",
        "obsv_50",
        "obsv_64",
        "fcast_34",
        "fcast_50",
        "fcast_64",
    ]:
        if _c not in _tbl.columns:
            _tbl[_c] = pd.NA
    _disp = _tbl[
        [
            "Storm",
            "obsv_34",
            "obsv_50",
            "obsv_64",
            "fcast_34",
            "fcast_50",
            "fcast_64",
        ]
    ].rename(
        columns={
            "obsv_34": "Obsv 34kt",
            "obsv_50": "Obsv 50kt",
            "obsv_64": "Obsv 64kt",
            "fcast_34": "Fcast 34kt",
            "fcast_50": "Fcast 50kt",
            "fcast_64": "Fcast 64kt",
        }
    )

    def _fmt(x):
        return f"{int(x):,}" if pd.notna(x) and x > 0 else "—"

    mo.output.replace(
        mo.Html(
            _disp.style.format(
                {c: _fmt for c in _disp.columns if c != "Storm"}
            )
            .bar(subset=["Obsv 34kt", "Fcast 34kt"], color="#aed6f1", vmin=0)
            .set_properties(**{"text-align": "center"})
            .set_properties(subset=["Storm"], **{"text-align": "left"})
            .set_table_styles(
                [{"selector": "th", "props": [("text-align", "center")]}]
            )
            .hide(axis="index")
            .to_html()
        )
    )
    return


@app.cell
def load_codab(load_codab_from_blob):
    gdf_hti = load_codab_from_blob(admin_level=0)
    return (gdf_hti,)


@app.cell
def load_nhc_alerts(pd):
    _ALERTS = [
        {
            "name": "LILI",
            "season": 2002,
            "ts_watch": True,
            "ts_warning": False,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "CHARLEY",
            "season": 2004,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "IVAN",
            "season": 2004,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": True,
            "hur_warning": False,
        },
        {
            "name": "ALPHA",
            "season": 2005,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "ERNESTO",
            "season": 2006,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "DEAN",
            "season": 2007,
            "ts_watch": True,
            "ts_warning": False,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "NOEL",
            "season": 2007,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "FAY",
            "season": 2008,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "GUSTAV",
            "season": 2008,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": True,
            "hur_warning": True,
        },
        {
            "name": "HANNA",
            "season": 2008,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "IKE",
            "season": 2008,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "TOMAS",
            "season": 2010,
            "ts_watch": False,
            "ts_warning": False,
            "hur_watch": False,
            "hur_warning": True,
        },
        {
            "name": "IRENE",
            "season": 2011,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": True,
            "hur_warning": False,
        },
        {
            "name": "SANDY",
            "season": 2012,
            "ts_watch": True,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "MATTHEW",
            "season": 2016,
            "ts_watch": True,
            "ts_warning": True,
            "hur_watch": True,
            "hur_warning": True,
        },
        {
            "name": "IRMA",
            "season": 2017,
            "ts_watch": True,
            "ts_warning": True,
            "hur_watch": True,
            "hur_warning": True,
        },
        {
            "name": "LAURA",
            "season": 2020,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "ELSA",
            "season": 2021,
            "ts_watch": False,
            "ts_warning": True,
            "hur_watch": False,
            "hur_warning": True,
        },
        {
            "name": "GRACE",
            "season": 2021,
            "ts_watch": True,
            "ts_warning": False,
            "hur_watch": False,
            "hur_warning": False,
        },
        {
            "name": "BERYL",
            "season": 2024,
            "ts_watch": True,
            "ts_warning": False,
            "hur_watch": True,
            "hur_warning": False,
        },
        {
            "name": "MELISSA",
            "season": 2025,
            "ts_watch": True,
            "ts_warning": True,
            "hur_watch": True,
            "hur_warning": False,
        },
    ]
    df_nhc_alerts = pd.DataFrame(_ALERTS)
    return (df_nhc_alerts,)


@app.cell
def trigger_table(
    df_action_trig,
    df_exp,
    df_impact,
    df_nhc_alerts,
    df_old_trig,
    df_total_exp,
    mo,
    pd,
):
    _n = 12

    # One row per storm, one column per wind threshold
    _exp_pivot = (
        df_exp.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="pop_exposed",
            aggfunc="first",
        )
        .rename(columns={34: "exp_34", 50: "exp_50", 64: "exp_64"})
        .reset_index()
    )
    for _c in ["exp_34", "exp_50", "exp_64"]:
        if _c not in _exp_pivot.columns:
            _exp_pivot[_c] = 0

    _texp_pivot = (
        df_total_exp.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="max_total_exposure",
            aggfunc="max",
        )
        .rename(columns={34: "total_34", 50: "total_50", 64: "total_64"})
        .reset_index()
    )
    for _c in ["total_34", "total_50", "total_64"]:
        if _c not in _texp_pivot.columns:
            _texp_pivot[_c] = 0

    # n-th largest exposure = threshold that selects exactly n storms
    def _get_thresh(col: str) -> float:
        _vals = _exp_pivot[col].fillna(0).sort_values(ascending=False)
        return float(_vals.iloc[_n - 1]) if _n <= len(_vals) else 0.0

    _thresh = {
        34: _get_thresh("exp_34"),
        50: _get_thresh("exp_50"),
        64: _get_thresh("exp_64"),
    }

    def _get_thresh_total(col: str) -> float:
        _vals = _df_tmp[col].fillna(0).sort_values(ascending=False)
        return float(_vals.iloc[_n - 1]) if _n <= len(_vals) else 0.0

    _n_years = int(df_exp["season"].max() - df_exp["season"].min() + 1)
    _rp = (_n_years + 1) / _n

    # Build combined dataframe: start with wind exposure + metadata
    _meta = df_exp[["sid", "season", "name"]].drop_duplicates("sid")
    _df = _meta.merge(_exp_pivot, on="sid", how="outer")
    _df = _df.merge(_texp_pivot, on="sid", how="left")
    for _c in ["total_34", "total_50", "total_64"]:
        _df[_c] = _df[_c].fillna(0)
    # Compute total thresholds now that _df has total columns
    _df_tmp = _df
    _thresh_total = {
        34: _get_thresh_total("total_34"),
        50: _get_thresh_total("total_50"),
        64: _get_thresh_total("total_64"),
    }
    # Merge CERF/impact data by sid
    _df = _df.merge(df_impact, on="sid", how="outer")
    _df = _df.drop_duplicates(subset=["sid"])
    _df["season"] = pd.to_numeric(
        _df["season"].fillna(_df["sid"].str[:4]), errors="coerce"
    ).astype("Int64")
    if "Event Name" in _df.columns:
        _df["name"] = _df["name"].fillna(_df["Event Name"])

    # Merge historical trigger flags + EM-DAT totals by (name, season)
    _df["_name_key"] = _df["name"].str.strip().str.upper()
    _trig_lookup = df_old_trig.copy()
    _trig_lookup["_name_key"] = _trig_lookup["name"].str.strip().str.upper()
    _df = _df.merge(
        _trig_lookup[
            [
                "_name_key",
                "season",
                "mob_trig",
                "obsv_trig",
                "Total Affected",
                "Amount in US$",
            ]
        ],
        on=["_name_key", "season"],
        how="left",
        suffixes=("", "_hist"),
    )
    # Official WG values take precedence over blob values
    for _hcol in ["Total Affected", "Amount in US$"]:
        _hcol_hist = f"{_hcol}_hist"
        if _hcol_hist in _df.columns:
            _df[_hcol] = _df[_hcol_hist].combine_first(_df[_hcol])
            _df = _df.drop(columns=[_hcol_hist])
    _df = _df.drop(columns=["_name_key"])

    # Merge NHC watch/warning alerts
    _df["_nk"] = _df["name"].str.strip().str.upper()
    _al = df_nhc_alerts.copy()
    _al["_nk"] = _al["name"].str.upper()
    _df = _df.merge(
        _al[
            [
                "_nk",
                "season",
                "ts_watch",
                "ts_warning",
                "hur_watch",
                "hur_warning",
            ]
        ],
        on=["_nk", "season"],
        how="left",
    )
    _df = _df.drop(columns=["_nk"])
    for _c in ["ts_watch", "ts_warning", "hur_watch", "hur_warning"]:
        _df[_c] = _df[_c].fillna(False).astype(bool)

    _df = _df.merge(
        df_action_trig[["sid", "action_trig_old"]], on="sid", how="left"
    )
    _df["mob_trig"] = (
        _df["mob_trig"].astype("boolean").fillna(False).astype(bool)
    )
    _df["obsv_trig"] = (
        _df["obsv_trig"].astype("boolean").fillna(False).astype(bool)
    )
    _df["action_trig_old"] = (
        _df["action_trig_old"].astype("boolean").fillna(False).astype(bool)
    )

    # Keep storms with impact > 0, CERF funding, or non-zero wind exposure
    _has_emdat = _df["Total Affected"].notna() & (_df["Total Affected"] > 0)
    _has_cerf = _df["Amount in US$"].notna() & (_df["Amount in US$"] > 0)
    _has_exp = (
        (_df["exp_34"].fillna(0) > 0)
        | (_df["exp_50"].fillna(0) > 0)
        | (_df["exp_64"].fillna(0) > 0)
    )
    _df = _df[_has_emdat | _has_cerf | _has_exp].copy()

    _df["trig_34"] = _df["exp_34"].fillna(0) >= _thresh[34]
    _df["trig_50"] = _df["exp_50"].fillna(0) >= _thresh[50]
    _df["trig_64"] = _df["exp_64"].fillna(0) >= _thresh[64]
    _df["trig_total_34"] = _df["total_34"].fillna(0) >= _thresh_total[34]
    _df["trig_total_50"] = _df["total_50"].fillna(0) >= _thresh_total[50]
    _df["trig_total_64"] = _df["total_64"].fillna(0) >= _thresh_total[64]

    def _cerf_str(row):
        if pd.notna(row["Amount in US$"]) and row["Amount in US$"] > 0:
            return f"${row['Amount in US$']:,.0f}"
        if pd.notna(row["season"]) and int(row["season"]) == 2008:
            return "combined"
        if pd.notna(row["season"]) and int(row["season"]) >= 2006:
            return "—"
        return "pre-"

    _df["CERF"] = _df.apply(_cerf_str, axis=1)
    _df["Action"] = _df["action_trig_old"].map({True: "✓", False: "—"})
    _df["Mob. trig."] = _df["mob_trig"].map({True: "✓", False: "—"})
    _df["Obsv. trig."] = _df["obsv_trig"].map({True: "✓", False: "—"})

    def _storm_label(row):
        if pd.notna(row["name"]):
            _nm = str(row["name"]).strip().title()
        elif str(row.get("sid", "")) == "2002265N10315":
            _nm = "Lili"  # 2002 hurricane, missing from EM-DAT for Haiti
        else:
            _nm = "Unnamed"
        return f"{_nm} ({row['season']})"

    _df["Storm"] = _df.apply(_storm_label, axis=1)
    _df = _df.sort_values(
        ["Total Affected", "Amount in US$", "exp_64", "exp_50", "exp_34"],
        ascending=False,
        na_position="last",
    )

    # Format alert booleans as checkmarks for display
    for _ac in ["ts_watch", "ts_warning", "hur_watch", "hur_warning"]:
        _df[f"_{_ac}_disp"] = _df[_ac].map({True: "✓", False: "—"})

    _display = (
        _df[
            [
                "Storm",
                "exp_34",
                "total_34",
                "exp_50",
                "total_50",
                "exp_64",
                "total_64",
                "trig_34",
                "trig_total_34",
                "trig_50",
                "trig_total_50",
                "trig_64",
                "trig_total_64",
                "Total Affected",
                "CERF",
                "Action",
                "Mob. trig.",
                "Obsv. trig.",
                "_ts_watch_disp",
                "_ts_warning_disp",
                "_hur_watch_disp",
                "_hur_warning_disp",
            ]
        ]
        .rename(
            columns={
                "exp_34": "34 kt final obsv",
                "total_34": "34 kt max total fcast",
                "exp_50": "50 kt final obsv",
                "total_50": "50 kt max total fcast",
                "exp_64": "64 kt final obsv",
                "total_64": "64 kt max total fcast",
                "_ts_watch_disp": "TS Watch",
                "_ts_warning_disp": "TS Warning",
                "_hur_watch_disp": "Hur. Watch",
                "_hur_warning_disp": "Hur. Warning",
            }
        )
        .reset_index(drop=True)
    )

    def _style_row(row):
        _styles = [""] * len(row)
        _idx = list(row.index)
        for _oc, _to, _tr, _tt in [
            (
                "34 kt final obsv",
                "34 kt max total fcast",
                "trig_34",
                "trig_total_34",
            ),
            (
                "50 kt final obsv",
                "50 kt max total fcast",
                "trig_50",
                "trig_total_50",
            ),
            (
                "64 kt final obsv",
                "64 kt max total fcast",
                "trig_64",
                "trig_total_64",
            ),
        ]:
            if _tr in _idx and row[_tr] and _oc in _idx:
                _styles[
                    _idx.index(_oc)
                ] = "background-color: gold; font-weight: bold"
            if _tt in _idx and row[_tt] and _to in _idx:
                _styles[
                    _idx.index(_to)
                ] = "background-color: #a8d8ea; font-weight: bold"
        return _styles

    def _style_check(val):
        if val == "✓":
            return (
                "background-color: #fff0b3; color: #888; font-weight: normal"
            )
        return "color: #ccc"

    def _style_cerf(val):
        if isinstance(val, str) and val.startswith("$"):
            return "background-color: crimson; color: white; font-weight: bold"
        if val == "combined":
            return "background-color: crimson; color: white; font-weight: bold"
        if val == "—":
            return "background-color: #cce5ff; color: #555"
        return "color: #aaa"

    def _style_alert(val):
        if val == "✓":
            return (
                "background-color: #ffcccc; font-weight: bold; color: #800000"
            )
        return "color: #ccc"

    def _style_hur_warn(val):
        if val == "✓":
            return "background-color: crimson; color: white; font-weight: bold"
        return "color: #ccc"

    _styled = (
        _display.style.apply(_style_row, axis=1)
        .map(_style_check, subset=["Action", "Mob. trig.", "Obsv. trig."])
        .map(_style_cerf, subset=["CERF"])
        .map(_style_alert, subset=["TS Watch", "TS Warning", "Hur. Watch"])
        .map(_style_hur_warn, subset=["Hur. Warning"])
        .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
        .format(
            {
                c: lambda x: f"{int(x):,}" if pd.notna(x) and x > 0 else "—"
                for c in [
                    "34 kt final obsv",
                    "34 kt max total fcast",
                    "50 kt final obsv",
                    "50 kt max total fcast",
                    "64 kt final obsv",
                    "64 kt max total fcast",
                ]
            }
            | {"Total Affected": lambda x: f"{x:,.0f}" if pd.notna(x) else "—"}
        )
        .hide(
            axis="columns",
            subset=[
                "trig_34",
                "trig_total_34",
                "trig_50",
                "trig_total_50",
                "trig_64",
                "trig_total_64",
            ],
        )
        .hide(axis="index")
    )

    _summary = mo.md(
        f"**Return period: {_rp:.1f} yrs** ({_n} storms / {_n_years} yrs)  \n"
        f"Final obsv thresholds (gold): **34 kt** ≥ {int(_thresh[34]):,} · "
        f"**50 kt** ≥ {int(_thresh[50]):,} · "
        f"**64 kt** ≥ {int(_thresh[64]):,}  \n"
        f"Max total fcast thresholds (blue): **34 kt** ≥ {int(_thresh_total[34]):,} · "
        f"**50 kt** ≥ {int(_thresh_total[50]):,} · "
        f"**64 kt** ≥ {int(_thresh_total[64]):,} people exposed"
    )

    mo.output.replace(mo.vstack([_summary, mo.Html(_styled.to_html())]))
    df_triggers = _df
    thresh = _thresh
    return df_triggers, thresh


@app.cell
def storm_selector(df_exp, mo, pd):
    _exposed_sids = df_exp[df_exp["pop_exposed"] > 0]["sid"].unique()
    _storms = (
        df_exp[df_exp["sid"].isin(_exposed_sids)][["sid", "season", "name"]]
        .drop_duplicates("sid")
        .copy()
    )

    def _label(row):
        _name = (
            str(row["name"]).strip().title() if pd.notna(row["name"]) else ""
        )
        _yr = int(row["season"]) if pd.notna(row["season"]) else row["sid"][:4]
        return (
            f"{_name} ({_yr}) — {row['sid']}"
            if _name
            else f"({_yr}) — {row['sid']}"
        )

    _storms["label"] = _storms.apply(_label, axis=1)
    _storms = _storms.sort_values("season", ascending=False)
    _storm_map = dict(zip(_storms["label"], _storms["sid"]))

    _default_melissa = "2025291N11319"
    _default_key = next(
        (k for k, v in _storm_map.items() if v == _default_melissa), None
    )

    storm_sel = mo.ui.dropdown(
        options=_storm_map, value=_default_key, label="Select storm for map"
    )
    storm_sel
    return (storm_sel,)


@app.cell
def storm_map(
    df_exp,
    df_mon_all,
    df_obs_rain,
    df_total_exp,
    gdf_hti,
    gpd,
    mo,
    mpatches,
    pd,
    plt,
    storm_sel,
    stratus,
    text,
):
    _sid = storm_sel.value
    mo.stop(
        _sid is None,
        mo.md("Select a storm above to view the wind buffer map."),
    )

    _dev_engine = stratus.get_engine(stage="dev")
    with _dev_engine.connect() as _con:
        _gdf_bufs = gpd.read_postgis(
            text(
                "SELECT sid, wind_speed_kt, geometry"
                " FROM storms.ibtracs_wind_buffers"
                " WHERE sid = :sid"
            ),
            _con,
            geom_col="geometry",
            params={"sid": _sid},
        )
        # Get atcf_id directly from DB — works even for storms not in monitors parquet
        _df_atcf = pd.read_sql(
            text("SELECT atcf_id FROM storms.ibtracs_storms WHERE sid = :sid"),
            _con,
            params={"sid": _sid},
        )
        _atcf_id = _df_atcf["atcf_id"].iloc[0] if not _df_atcf.empty else None
        if _atcf_id is not None:
            _df_fexp_ts = pd.read_sql(
                text(
                    """
                    SELECT issued_time, wind_speed_kt, pop_exposed
                    FROM storms.nhc_tracks_fcastonly_exposure
                    WHERE atcf_id = :atcf_id AND iso3 = 'HTI' AND admin_level = 0
                    ORDER BY issued_time
                """
                ),
                _con,
                params={"atcf_id": _atcf_id},
            )
            _df_oexp_ts = pd.read_sql(
                text(
                    """
                    SELECT valid_time, wind_speed_kt, pop_exposed
                    FROM storms.nhc_tracks_obsv_exposure
                    WHERE atcf_id = :atcf_id AND iso3 = 'HTI' AND admin_level = 0
                    ORDER BY valid_time
                """
                ),
                _con,
                params={"atcf_id": _atcf_id},
            )
        else:
            _df_fexp_ts = pd.DataFrame(
                columns=["issued_time", "wind_speed_kt", "pop_exposed"]
            )
            _df_oexp_ts = pd.DataFrame(
                columns=["valid_time", "wind_speed_kt", "pop_exposed"]
            )
    _dev_engine.dispose()

    _storm_mon = df_mon_all[df_mon_all["sid"] == _sid]

    # Compute total_exp time series (pre-cutoff fcast only + cumul. obsv)
    _storm_mon_pre = (
        _storm_mon[~_storm_mon["past_cutoff"]][["issue_time"]]
        .drop_duplicates()
        .rename(columns={"issue_time": "issued_time"})
    )
    _df_total_ts_list = []
    for _wt in [34, 50, 64]:
        _f = _df_fexp_ts[_df_fexp_ts["wind_speed_kt"] == _wt].sort_values(
            "issued_time"
        )
        if not _storm_mon_pre.empty:
            _f = _f.merge(_storm_mon_pre, on="issued_time", how="inner")
        if _f.empty:
            continue
        _o = _df_oexp_ts[_df_oexp_ts["wind_speed_kt"] == _wt]
        if _o.empty:
            _f = _f.copy()
            _f["total_exp"] = _f["pop_exposed"]
        else:
            _os = _o.sort_values("valid_time").assign(
                _cm=lambda x: x["pop_exposed"].cummax()
            )
            _m = pd.merge_asof(
                _f[["issued_time", "pop_exposed"]],
                _os[["valid_time", "_cm"]].rename(
                    columns={"valid_time": "issued_time"}
                ),
                on="issued_time",
                direction="backward",
            )
            _f = _f.copy()
            _f["total_exp"] = (
                _m["pop_exposed"].values + _m["_cm"].fillna(0).values
            )
        _f["wind_speed_kt"] = _wt
        _df_total_ts_list.append(
            _f[["issued_time", "wind_speed_kt", "total_exp"]]
        )
    _df_total_ts = (
        pd.concat(_df_total_ts_list, ignore_index=True)
        if _df_total_ts_list
        else pd.DataFrame(
            columns=["issued_time", "wind_speed_kt", "total_exp"]
        )
    )

    _row = df_exp[df_exp["sid"] == _sid]
    _name = (
        str(_row.iloc[0]["name"]).strip().title()
        if not _row.empty and pd.notna(_row.iloc[0]["name"])
        else ""
    )
    _yr = (
        int(_row.iloc[0]["season"])
        if not _row.empty and pd.notna(_row.iloc[0]["season"])
        else _sid[:4]
    )
    _title = f"{_name} ({_yr})" if _name else _sid[:4]
    _exp_by_wt = (
        _row[_row["pop_exposed"] > 0]
        .set_index("wind_speed_kt")["pop_exposed"]
        .to_dict()
    )

    # Cutoff and closest-approach times from monitors
    _cutoff_time = None
    _approach_time = None
    if not _storm_mon.empty:
        _pre = _storm_mon[~_storm_mon["past_cutoff"]]
        if not _pre.empty:
            _cutoff_time = _pre["issue_time"].max()
        _obsv_rows = _storm_mon[_storm_mon["lt_name"] == "obsv"]
        if not _obsv_rows.empty:
            _approach_time = _obsv_rows["issue_time"].min()

    # Precipitation time series from monitors (action lead)
    _prec_ts = _storm_mon[_storm_mon["lt_name"] == "action"][
        ["issue_time", "roll2_rain_dist", "past_cutoff"]
    ].sort_values("issue_time")
    _obs_rain_val = (
        df_obs_rain.loc[df_obs_rain["sid"] == _sid, "max_obs_rain"].iloc[0]
        if _sid in df_obs_rain["sid"].values
        else None
    )

    # ── Figure layout ────────────────────────────────────────────────────
    _cmap = plt.get_cmap("YlOrRd")
    _norm = plt.Normalize(vmin=20, vmax=80)
    _WIND_SPEEDS = [34, 50, 64]
    _WKT_COLORS = {
        34: _cmap(_norm(34)),
        50: _cmap(_norm(50)),
        64: _cmap(_norm(64)),
    }

    _fig = plt.figure(figsize=(22, 7), dpi=120)
    _gs = _fig.add_gridspec(
        2, 2, width_ratios=[1.1, 1.9], hspace=0.45, wspace=0.3
    )
    _ax_map = _fig.add_subplot(_gs[:, 0])
    _ax_exp = _fig.add_subplot(_gs[0, 1])
    _ax_prec = _fig.add_subplot(_gs[1, 1])

    # ── Map ──────────────────────────────────────────────────────────────
    gdf_hti.boundary.plot(ax=_ax_map, linewidth=0.8, color="k")
    _patches = []
    for _wt in _WIND_SPEEDS:
        if "wind_speed_kt" not in _gdf_bufs.columns:
            continue
        _buf = _gdf_bufs[
            (_gdf_bufs["wind_speed_kt"] == _wt)
            & _gdf_bufs.geometry.notna()
            & ~_gdf_bufs.geometry.is_empty
        ]
        if not _buf.empty:
            _color = _cmap(_norm(_wt))
            _buf.plot(
                ax=_ax_map,
                alpha=0.4,
                color=_color,
                edgecolor=_color,
                linewidth=0.5,
            )
            _exp_str = (
                f" — {int(_exp_by_wt[_wt]):,}" if _wt in _exp_by_wt else ""
            )
            _patches.append(
                mpatches.Patch(color=_color, label=f"{_wt} kt{_exp_str}")
            )
    if _patches:
        _ax_map.legend(handles=_patches[::-1], loc="lower left", fontsize=8)
    else:
        _ax_map.text(
            0.5,
            0.5,
            "No wind buffer data",
            transform=_ax_map.transAxes,
            ha="center",
            va="center",
            color="grey",
        )
    _minx, _miny, _maxx, _maxy = gdf_hti.total_bounds
    _pad = 3
    _ax_map.set_xlim(_minx - _pad, _maxx + _pad)
    _ax_map.set_ylim(_miny - _pad, _maxy + _pad)
    _ax_map.set_title(_title, fontsize=11)
    _ax_map.set_axis_off()

    # ── Total exposure evolution (pre-cutoff fcast + cumul. obsv) ────────
    _total_by_wt = (
        df_total_exp[df_total_exp["sid"] == _sid]
        .set_index("wind_speed_kt")["max_total_exposure"]
        .to_dict()
        if _sid in df_total_exp["sid"].values
        else {}
    )
    if not _df_total_ts.empty:
        for _wt in _WIND_SPEEDS:
            _sub = _df_total_ts[
                _df_total_ts["wind_speed_kt"] == _wt
            ].sort_values("issued_time")
            if not _sub.empty:
                _ax_exp.plot(
                    _sub["issued_time"],
                    _sub["total_exp"],
                    color=_WKT_COLORS[_wt],
                    marker="o",
                    markersize=3,
                    linewidth=1.2,
                    label=f"{_wt} kt",
                )
        for _wt in _WIND_SPEEDS:
            if _wt in _exp_by_wt:
                _ax_exp.axhline(
                    _exp_by_wt[_wt],
                    color=_WKT_COLORS[_wt],
                    linestyle=":",
                    linewidth=1.5,
                    alpha=0.8,
                )
    elif _total_by_wt:
        for _wt in _WIND_SPEEDS:
            if _wt in _total_by_wt:
                _ax_exp.axhline(
                    _total_by_wt[_wt],
                    color=_WKT_COLORS[_wt],
                    linestyle="--",
                    linewidth=1.5,
                    alpha=0.9,
                    label=f"{_wt} kt max: {int(_total_by_wt[_wt]):,}",
                )
    # Vertical lines
    if _cutoff_time is not None:
        _ax_exp.axvline(
            _cutoff_time,
            color="crimson",
            linestyle="--",
            linewidth=1.2,
            label="Cutoff",
        )
    if _approach_time is not None:
        _ax_exp.axvline(
            _approach_time,
            color="#333",
            linestyle="-",
            linewidth=1.2,
            label="Closest approach",
        )
    _ax_exp.set_title(
        "Total exposure evolution (fcast + cumul. obsv, pre-cutoff)",
        fontsize=9,
    )
    _ax_exp.set_ylabel("Pop. exposed")
    _ax_exp.tick_params(axis="x", labelsize=7, rotation=20)
    _exp_handles, _exp_labels = _ax_exp.get_legend_handles_labels()
    if _exp_handles:
        _ax_exp.legend(fontsize=7, loc="upper left")
    _ax_exp.grid(True, alpha=0.25, linestyle="--")
    _ax_exp.set_ylim(bottom=0)

    # ── Forecast precipitation evolution ─────────────────────────────────
    if not _prec_ts.empty:
        _pre_prec = _prec_ts[~_prec_ts["past_cutoff"]]
        _post_prec = _prec_ts[_prec_ts["past_cutoff"]]
        if not _pre_prec.empty:
            _ax_prec.plot(
                _pre_prec["issue_time"],
                _pre_prec["roll2_rain_dist"],
                color="steelblue",
                marker="o",
                markersize=3,
                linewidth=1.2,
                label="Forecast (pre-cutoff)",
            )
        if not _post_prec.empty:
            _ax_prec.plot(
                _post_prec["issue_time"],
                _post_prec["roll2_rain_dist"],
                color="steelblue",
                marker="o",
                markersize=3,
                linewidth=1.2,
                linestyle="--",
                alpha=0.4,
                label="Forecast (post-cutoff)",
            )
    if _obs_rain_val is not None:
        _ax_prec.axhline(
            _obs_rain_val,
            color="darkorange",
            linestyle=":",
            linewidth=1.5,
            label=f"Observed ({_obs_rain_val:.0f} mm)",
        )
    if _cutoff_time is not None:
        _ax_prec.axvline(
            _cutoff_time,
            color="crimson",
            linestyle="--",
            linewidth=1.2,
            label="Cutoff",
        )
    if _approach_time is not None:
        _ax_prec.axvline(
            _approach_time,
            color="#333",
            linestyle="-",
            linewidth=1.2,
            label="Closest approach",
        )
    _ax_prec.set_title(
        "Forecast rainfall evolution — action lead (dotted = observed)",
        fontsize=9,
    )
    _ax_prec.set_ylabel("Roll-2d rain (mm)")
    _ax_prec.tick_params(axis="x", labelsize=7, rotation=20)
    _ax_prec.legend(fontsize=7, loc="upper left")
    _ax_prec.grid(True, alpha=0.25, linestyle="--")
    _ax_prec.set_ylim(bottom=0)

    _fig.suptitle(_title, fontsize=12, y=1.01)
    _fig
    return


@app.cell
def corr_plots(df_triggers, plt, thresh):
    _fig, _axes = plt.subplots(1, 3, figsize=(15, 5), dpi=150, sharey=True)

    for _ax, (_wt, _col, _trig_col, _label) in zip(
        _axes,
        [
            (34, "exp_34", "trig_34", "34 kt"),
            (50, "exp_50", "trig_50", "50 kt"),
            (64, "exp_64", "trig_64", "64 kt"),
        ],
    ):
        _sub = df_triggers[
            (df_triggers[_col].fillna(0) > 0)
            & df_triggers["Total Affected"].notna()
            & (df_triggers["Total Affected"] > 0)
        ][["Storm", _col, "Total Affected", _trig_col]].copy()

        _r = _sub[_col].corr(_sub["Total Affected"])

        _ax.scatter(_sub[_col], _sub["Total Affected"], alpha=0)

        for _, _row in _sub.iterrows():
            _ax.annotate(
                _row["Storm"],
                xy=(_row[_col], _row["Total Affected"]),
                ha="center",
                va="center",
                fontsize=7,
                color="crimson" if _row[_trig_col] else "#999999",
            )

        _ax.axvline(
            thresh[_wt],
            color="crimson",
            linewidth=1,
            linestyle="--",
            label=f"Threshold: {int(thresh[_wt]):,}",
        )
        _ax.legend(fontsize=7, loc="upper left")

        _ax.set_xlim(left=0)
        _ax.set_ylim(bottom=0)
        _ax.set_xlabel(f"Pop. exposed ({_label})")
        _ax.set_title(f"{_label} exposure vs. Total Affected  (r = {_r:.2f})")
        _ax.grid(True, alpha=0.3, linestyle="--")
        if _ax is _axes[0]:
            _ax.set_ylabel("Total Affected (EM-DAT)")

    _fig.suptitle("Wind exposure vs. EM-DAT Total Affected", y=1.01)
    plt.tight_layout()
    _fig
    return


@app.cell
def trigger_corr_table(df_rain_opt, plt):
    _cols = {
        "total_exp_34": "Total exp 34",
        "total_exp_50": "Total exp 50",
        "total_exp_64": "Total exp 64",
        "max_fcast_rain": "Fcast rain",
        "max_obs_rain": "Obs rain",
        "action_trig_old": "Old action",
        "obsv_trig": "Old obsv",
        "Total Affected": "Impact",
        "has_cerf": "CERF",
    }
    _df = df_rain_opt[[c for c in _cols if c in df_rain_opt.columns]].copy()
    for _c in ["action_trig_old", "obsv_trig"]:
        if _c in _df.columns:
            _df[_c] = _df[_c].astype(float)
    if "has_cerf" in _df.columns:
        _df["has_cerf"] = _df["has_cerf"].astype(float)
    _df = _df.rename(columns=_cols)
    _corr = _df.corr(numeric_only=True).round(2)

    _fig, _ax = plt.subplots(figsize=(10, 8), dpi=120)
    _mat = _corr.values
    _im = _ax.imshow(_mat, cmap="RdBu_r", vmin=-1, vmax=1, aspect="auto")
    _fig.colorbar(_im, ax=_ax, fraction=0.03, pad=0.02)
    _labels = list(_corr.columns)
    _ax.set_xticks(range(len(_labels)))
    _ax.set_yticks(range(len(_labels)))
    _ax.set_xticklabels(_labels, rotation=45, ha="right", fontsize=8)
    _ax.set_yticklabels(_labels, fontsize=8)
    for _i in range(len(_labels)):
        for _j in range(len(_labels)):
            _v = _mat[_i, _j]
            _ax.text(
                _j,
                _i,
                f"{_v:.2f}",
                ha="center",
                va="center",
                fontsize=6.5,
                color="white" if abs(_v) > 0.5 else "#333",
            )
    _ax.set_title(
        "Pearson correlations — trigger indicators vs. CERF & impact",
        fontsize=10,
    )
    plt.tight_layout()
    _fig
    return


@app.cell
def doc_optimization(mo):
    mo.md(
        """
    ## Trigger optimization

    Tests a simplified **OR trigger** with three indicators:

    1. **Total exposure** — max(fcastonly exposure + cumulative observed exposure)
       across all pre-cutoff forecast issuances. **Fcastonly** is the *future-only*
       forecast: the NHC wind-buffer geometry minus the observed swath already
       accumulated up to that issuance, so the two components are non-overlapping
       and sum to total storm exposure without double-counting. Source tables:
       `nhc_tracks_fcastonly_exposure` (future cone only) and
       `nhc_tracks_obsv_exposure` (already-swept area), both iso3 = HTI,
       admin_level = 0, aligned via `pd.merge_asof`. Only issuances more than 48 h
       before closest approach are included (same window as the existing Haiti
       action trigger).

    2. **Forecast rainfall** — max 2-day rolling rainfall from NHC monitors at
       action lead time (pre-cutoff only), from `nhc.load_hist_fcast_monitors`.

    3. **Observed rainfall** — max 2-day rolling rainfall from the 0-day hindcast
       rows of the same monitor archive.

    A storm **triggers** if *any* of the three indicators meets its threshold. Three
    wind levels are tested (34 / 50 / 64 kt) as separate options.

    **How thresholds are determined:** a 2D sweep over exposure threshold and
    forecast-rainfall threshold. For each combination, the observed-rainfall
    threshold is set *deterministically* as the (n − n_exp − n_fcast)-th largest
    observed rainfall among storms not already triggered by exposure or forecast
    rainfall. This guarantees exactly n = 12 storms trigger overall (RP ≈ 2.1 yrs
    over 2002–2025). The best option per wind level maximises CERF storm count,
    then Total Affected, then minimises the exposure threshold.

    **Condition columns in the storm table:**

    | Column | Meaning |
    |--------|---------|
    | **X kt exp** | Total exposure ≥ optimised exposure threshold at this wind level |
    | **X kt fcast** | Forecast rainfall ≥ optimised forecast-rainfall threshold |
    | **X kt rain** | Observed rainfall ≥ optimised observed-rainfall threshold |
    | **X kt+O** | Combined: any condition met (the actual trigger) |
    """
    )
    return


@app.cell
def rain_trigger_opt(
    df_action_trig,
    df_exp,
    df_obs_rain,
    df_old_trig,
    df_rain,
    df_total_exp,
    mo,
    pd,
):
    _n = 12  # RP ≈ 2.1 yrs over 2002-2025

    # ── Build base data frame ─────────────────────────────────────────────  # noqa: E501
    _exp_pivot = (
        df_exp.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="pop_exposed",
            aggfunc="first",
        )
        .rename(columns={34: "exp_34", 50: "exp_50", 64: "exp_64"})
        .reset_index()
    )
    for _c in ["exp_34", "exp_50", "exp_64"]:
        if _c not in _exp_pivot.columns:
            _exp_pivot[_c] = 0

    _texp_pivot = (
        df_total_exp.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="max_total_exposure",
            aggfunc="max",
        )
        .rename(
            columns={
                34: "total_exp_34",
                50: "total_exp_50",
                64: "total_exp_64",
            }
        )
        .reset_index()
    )
    for _c in ["total_exp_34", "total_exp_50", "total_exp_64"]:
        if _c not in _texp_pivot.columns:
            _texp_pivot[_c] = 0

    _meta = df_exp[["sid", "season", "name"]].drop_duplicates("sid")
    _opt = _meta.merge(_exp_pivot, on="sid", how="outer")
    _opt = _opt.merge(_texp_pivot, on="sid", how="outer")

    # Merge old trigger flags by (name, season)
    _opt["_name_key"] = _opt["name"].str.strip().str.upper()
    _trig_lkp = df_old_trig.copy()
    _trig_lkp["_name_key"] = _trig_lkp["name"].str.strip().str.upper()
    _opt = _opt.merge(
        _trig_lkp[
            [
                "_name_key",
                "season",
                "mob_trig",
                "obsv_trig",
                "Total Affected",
                "Amount in US$",
            ]
        ],
        on=["_name_key", "season"],
        how="left",
        suffixes=("", "_hist"),
    )
    for _hcol in ["Total Affected", "Amount in US$"]:
        _hc = f"{_hcol}_hist"
        if _hc in _opt.columns:
            _opt[_hcol] = _opt[_hc].combine_first(_opt[_hcol])
            _opt = _opt.drop(columns=[_hc])
    _opt = _opt.drop(columns=["_name_key"])
    _opt = _opt.merge(
        df_action_trig[["sid", "action_trig_old"]], on="sid", how="left"
    )

    # Forecast and observed rainfall
    _fcast_rain = (
        df_rain[df_rain["lt_name"] == "action"]
        .groupby("sid")["max_rain"]
        .max()
        .reset_index()
        .rename(columns={"max_rain": "max_fcast_rain"})
    )
    _opt = _opt.merge(_fcast_rain, on="sid", how="left")
    _opt = _opt.merge(
        df_obs_rain[["sid", "max_obs_rain"]], on="sid", how="left"
    )

    _opt = _opt.drop_duplicates("sid")
    for _c in [
        "exp_34",
        "exp_50",
        "exp_64",
        "total_exp_34",
        "total_exp_50",
        "total_exp_64",
    ]:
        _opt[_c] = _opt[_c].fillna(0)
    _opt["season"] = pd.to_numeric(
        _opt["season"].fillna(_opt["sid"].str[:4]), errors="coerce"
    ).astype("Int64")
    _opt["max_obs_rain"] = _opt["max_obs_rain"].fillna(0)
    _opt["has_cerf"] = _opt["Amount in US$"].notna() & (
        _opt["Amount in US$"] > 0
    )
    _opt["mob_trig"] = (
        _opt["mob_trig"].astype("boolean").fillna(False).astype(bool)
    )
    _opt["obsv_trig"] = (
        _opt["obsv_trig"].astype("boolean").fillna(False).astype(bool)
    )
    _opt["action_trig_old"] = (
        _opt["action_trig_old"].astype("boolean").fillna(False).astype(bool)
    )

    def _slbl(row):
        if pd.notna(row["name"]):
            _nm = str(row["name"]).strip().title()
        elif str(row.get("sid", "")) == "2002265N10315":
            _nm = "Lili"  # 2002 hurricane, missing from EM-DAT for Haiti
        else:
            _nm = "Unnamed"
        _yr = row["season"] if pd.notna(row["season"]) else row["sid"][:4]
        return f"{_nm} ({_yr})"

    _opt["Storm"] = _opt.apply(_slbl, axis=1)
    _opt["old_combined"] = _opt["mob_trig"] | _opt["obsv_trig"]

    _cerf_lkp = dict(zip(_opt["sid"], _opt["has_cerf"]))
    _aff_lkp = dict(zip(_opt["sid"], _opt["Total Affected"].fillna(0)))

    # ── 2D sweep: total_exp threshold × forecast-rain threshold ──────────  # noqa: E501
    _results = []
    for _wkt, _tcol in [
        (34, "total_exp_34"),
        (50, "total_exp_50"),
        (64, "total_exp_64"),
    ]:
        _exp_vals = sorted(_opt[_tcol].dropna().unique())
        _fcast_vals = sorted(
            _opt["max_fcast_rain"].dropna().unique(), reverse=True
        )
        for _e_thresh in _exp_vals:
            _exp_sids = set(_opt[_opt[_tcol] >= _e_thresh]["sid"])
            for _r_fcast in _fcast_vals:
                _fcast_sids = set(
                    _opt[
                        _opt["max_fcast_rain"].notna()
                        & (_opt["max_fcast_rain"] >= _r_fcast)
                    ]["sid"]
                )
                _already = _exp_sids | _fcast_sids
                _n_obs = _n - len(_already)
                if _n_obs < 0:
                    continue
                _remaining = _opt[~_opt["sid"].isin(_already)]
                if _n_obs == 0:
                    _combined = frozenset(_already)
                    _results.append(
                        {
                            "wkt": _wkt,
                            "exp_thresh": _e_thresh,
                            "r_fcast_exact": _r_fcast,
                            "r_fcast": round(_r_fcast, 1),
                            "r_obs_exact": None,
                            "r_obs": None,
                            "n_exp": len(_exp_sids - _fcast_sids),
                            "n_fcast": len(_fcast_sids - _exp_sids),
                            "n_obs": 0,
                            "cerf_count": sum(
                                1
                                for _s in _combined
                                if _cerf_lkp.get(_s, False)
                            ),
                            "total_affected": sum(
                                _aff_lkp.get(_s, 0) for _s in _combined
                            ),
                            "_combined_sids": _combined,
                            "_exp_sids": frozenset(_exp_sids),
                            "_fcast_sids": frozenset(_fcast_sids),
                        }
                    )
                else:
                    _s = _remaining[
                        _remaining["max_obs_rain"].notna()
                    ].sort_values("max_obs_rain", ascending=False)
                    if len(_s) < _n_obs:
                        continue
                    _r_obs = float(_s.iloc[_n_obs - 1]["max_obs_rain"])
                    _obs_sids = set(
                        _remaining[
                            _remaining["max_obs_rain"].notna()
                            & (_remaining["max_obs_rain"] >= _r_obs)
                        ]["sid"]
                    )
                    if len(_obs_sids) != _n_obs:
                        continue
                    _combined = frozenset(_already | _obs_sids)
                    if len(_combined) != _n:
                        continue
                    _results.append(
                        {
                            "wkt": _wkt,
                            "exp_thresh": _e_thresh,
                            "r_fcast_exact": _r_fcast,
                            "r_fcast": round(_r_fcast, 1),
                            "r_obs_exact": _r_obs,
                            "r_obs": round(_r_obs, 1),
                            "n_exp": len(_exp_sids - _fcast_sids),
                            "n_fcast": len(_fcast_sids - _exp_sids),
                            "n_obs": _n_obs,
                            "cerf_count": sum(
                                1
                                for _s in _combined
                                if _cerf_lkp.get(_s, False)
                            ),
                            "total_affected": sum(
                                _aff_lkp.get(_s, 0) for _s in _combined
                            ),
                            "_combined_sids": _combined,
                            "_exp_sids": frozenset(_exp_sids),
                            "_fcast_sids": frozenset(_fcast_sids),
                        }
                    )

    df_rain_opt = _opt.copy()
    rain_opt_thresh = {}

    if not _results:
        mo.output.replace(mo.md("⚠ No valid combinations found."))
    else:
        _df_res = pd.DataFrame(_results)

        _df_options = (
            _df_res.sort_values(
                [
                    "wkt",
                    "n_exp",
                    "n_fcast",
                    "cerf_count",
                    "total_affected",
                    "exp_thresh",
                ],
                ascending=[True, True, True, False, False, True],
            )
            .groupby(["wkt", "n_exp", "n_fcast"], sort=True)
            .first()
            .reset_index()
        )
        _df_best = (
            _df_res.sort_values(
                ["wkt", "cerf_count", "total_affected", "exp_thresh"],
                ascending=[True, False, False, True],
            )
            .groupby("wkt", sort=True)
            .first()
            .reset_index()
        )

        _best_keys = set(
            zip(_df_best["wkt"], _df_best["n_exp"], _df_best["n_fcast"])
        )
        _df_options["best"] = [
            (r["wkt"], r["n_exp"], r["n_fcast"]) in _best_keys
            for _, r in _df_options.iterrows()
        ]

        for _, _brow in _df_best.iterrows():
            _wkt = int(_brow["wkt"])
            _opt[f"exp_trig_{_wkt}"] = _opt["sid"].isin(_brow["_exp_sids"])
            _opt[f"fcast_trig_{_wkt}"] = _opt["sid"].isin(_brow["_fcast_sids"])
            _opt[f"combined_{_wkt}"] = _opt["sid"].isin(
                _brow["_combined_sids"]
            )
        for _wkt in [34, 50, 64]:
            for _col in [
                f"exp_trig_{_wkt}",
                f"fcast_trig_{_wkt}",
                f"combined_{_wkt}",
            ]:
                if _col not in _opt.columns:
                    _opt[_col] = False

        _best_thresh = {}
        for _, _r in _df_best.iterrows():
            _wkt = int(_r["wkt"])
            _best_thresh[_wkt] = {
                "exp_thresh": int(_r["exp_thresh"]),
                "r_fcast": (
                    float(_r["r_fcast_exact"])
                    if pd.notna(_r["r_fcast_exact"])
                    else None
                ),
                "r_obs": (
                    float(_r["r_obs_exact"])
                    if pd.notna(_r["r_obs_exact"])
                    else None
                ),
            }
        rain_opt_thresh = _best_thresh

        _bool_hide = []
        for _wkt, _tcol in [
            (34, "total_exp_34"),
            (50, "total_exp_50"),
            (64, "total_exp_64"),
        ]:
            _t = _best_thresh.get(_wkt, {})
            _et = _t.get("exp_thresh", float("inf"))
            _rf = _t.get("r_fcast")
            _ro = _t.get("r_obs")
            _opt[f"_exp_flag_{_wkt}"] = _opt[_tcol] >= _et
            _opt[f"_fcast_flag_{_wkt}"] = (
                (
                    _opt["max_fcast_rain"].notna()
                    & (_opt["max_fcast_rain"] >= _rf)
                )
                if _rf is not None
                else pd.Series(False, index=_opt.index)
            )
            _opt[f"_rain_flag_{_wkt}"] = (
                (_opt["max_obs_rain"] >= _ro)
                if _ro is not None
                else pd.Series(False, index=_opt.index)
            )
            _opt[f"{_wkt} exp"] = _opt[f"_exp_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _opt[f"{_wkt} fcast"] = _opt[f"_fcast_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _opt[f"{_wkt} rain"] = _opt[f"_rain_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _opt[f"{_wkt}+O"] = _opt[f"combined_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _bool_hide += [
                f"_exp_flag_{_wkt}",
                f"_fcast_flag_{_wkt}",
                f"_rain_flag_{_wkt}",
                f"exp_trig_{_wkt}",
                f"fcast_trig_{_wkt}",
                f"combined_{_wkt}",
            ]

        df_rain_opt = _opt

        # ── Summary table ─────────────────────────────────────────────────  # noqa: E501
        _n_yrs = int(_opt["season"].max() - _opt["season"].min() + 1)
        _rp = (_n_yrs + 1) / _n
        _old_act_sids = set(_opt.loc[_opt["action_trig_old"], "sid"])
        _old_obs_sids = set(
            _opt.loc[_opt["obsv_trig"] & ~_opt["mob_trig"], "sid"]
        )
        _old_mob_sids = set(
            _opt.loc[_opt["mob_trig"] & ~_opt["action_trig_old"], "sid"]
        )
        _old_comb = _old_act_sids | _old_obs_sids | _old_mob_sids
        _n_old = len(_old_comb)
        _cerf_old = sum(1 for _s in _old_comb if _cerf_lkp.get(_s, False))
        _aff_old = int(sum(_aff_lkp.get(_s, 0) for _s in _old_comb))
        _rp_old = (_n_yrs + 1) / _n_old if _n_old else float("inf")

        _summary_rows = [
            {
                "Trigger": "Old (action|mob|obsv)",
                "Wind kt": "≥64 (dist)",
                "Exp thresh": "—",
                "Fcast rain mm": "≥42",
                "Obs rain mm": "≥70",
                "# Exp": len(_old_act_sids),
                "# Fcast": 0,
                "# Obs": len(_old_obs_sids),
                "CERF": _cerf_old,
                "Total Affected": _aff_old,
                "RP yrs": round(_rp_old, 1),
            }
        ]
        for _, _r in _df_best.iterrows():
            _wkt_b = int(_r["wkt"])
            _summary_rows.append(
                {
                    "Trigger": f"New {_wkt_b} kt ★",
                    "Wind kt": f"{_wkt_b}",
                    "Exp thresh": f"{int(_r['exp_thresh']):,}",
                    "Fcast rain mm": (
                        f"{_r['r_fcast']:.1f}"
                        if pd.notna(_r["r_fcast"])
                        else "—"
                    ),
                    "Obs rain mm": (
                        f"{_r['r_obs']:.1f}" if pd.notna(_r["r_obs"]) else "—"
                    ),
                    "# Exp": int(_r["n_exp"]),
                    "# Fcast": int(_r["n_fcast"]),
                    "# Obs": int(_r["n_obs"]),
                    "CERF": int(_r["cerf_count"]),
                    "Total Affected": int(_r["total_affected"]),
                    "RP yrs": round(_rp, 1),
                }
            )

        _df_sum = pd.DataFrame(_summary_rows)

        def _sty_sum(row):
            _ss = [""] * len(row)
            if "★" in str(row.get("Trigger", "")):
                _ss = ["background-color: #fffde7"] * len(row)
            return _ss

        _styl_sum = (
            _df_sum.style.apply(_sty_sum, axis=1)
            .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
            .format(
                {
                    "Total Affected": lambda x: f"{int(x):,}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .set_properties(**{"text-align": "center"})
            .set_properties(subset=["Trigger"], **{"text-align": "left"})
            .set_table_styles(
                [{"selector": "th", "props": [("text-align", "center")]}]
            )
            .hide(axis="index")
        )

        # ── Options table ─────────────────────────────────────────────────  # noqa: E501
        _df_opts_d = _df_options[
            [
                "wkt",
                "n_exp",
                "n_fcast",
                "exp_thresh",
                "r_fcast",
                "r_obs",
                "n_obs",
                "cerf_count",
                "total_affected",
                "best",
            ]
        ].copy()
        _df_opts_d["★"] = _df_opts_d["best"].map({True: "★", False: ""})
        _df_opts_d = _df_opts_d.rename(
            columns={
                "wkt": "Wind kt",
                "n_exp": "# Exp",
                "n_fcast": "# Fcast",
                "exp_thresh": "Exp thresh",
                "r_fcast": "Fcast rain mm",
                "r_obs": "Obs rain mm",
                "n_obs": "# Obs",
                "cerf_count": "CERF",
                "total_affected": "Total Aff.",
            }
        ).drop(columns=["best"])
        _styl_opts = (
            _df_opts_d.style.format(
                {
                    "Exp thresh": lambda x: f"{int(x):,}"
                    if pd.notna(x)
                    else "—",
                    "Fcast rain mm": lambda x: f"{x:.1f}"
                    if pd.notna(x)
                    else "—",
                    "Obs rain mm": lambda x: f"{x:.1f}"
                    if pd.notna(x)
                    else "—",
                    "Total Aff.": lambda x: f"{int(x):,}"
                    if pd.notna(x)
                    else "—",
                }
            )
            .set_properties(**{"text-align": "center"})
            .set_table_styles(
                [{"selector": "th", "props": [("text-align", "center")]}]
            )
            .hide(axis="index")
        )

        # ── Storm conditions table ────────────────────────────────────────  # noqa: E501
        def _cerf_s(row):
            if pd.notna(row.get("Amount in US$")) and row["Amount in US$"] > 0:
                return f"${row['Amount in US$']:,.0f}"
            if pd.notna(row.get("season")) and int(row["season"]) == 2008:
                return "combined"
            if pd.notna(row.get("season")) and int(row["season"]) >= 2006:
                return "—"
            return "pre-"

        _opt["CERF"] = _opt.apply(_cerf_s, axis=1)
        _opt["Old act."] = _opt["action_trig_old"].map({True: "✓", False: "—"})
        _opt["Old mob."] = _opt["mob_trig"].map({True: "✓", False: "—"})
        _opt["Old obsv."] = _opt["obsv_trig"].map({True: "✓", False: "—"})

        _cond_cols = [
            f"{w} {c}" for w in [34, 50, 64] for c in ["exp", "fcast", "rain"]
        ]
        _comb_cols = [f"{w}+O" for w in [34, 50, 64]]

        _any_trig = _opt[[f"combined_{w}" for w in [34, 50, 64]]].any(axis=1)
        _show = (
            _any_trig
            | _opt["mob_trig"]
            | _opt["obsv_trig"]
            | _opt["action_trig_old"]
            | (_opt["Total Affected"].fillna(0) > 0)
            | _opt["has_cerf"]
        )
        _storm_tbl = (
            _opt[_show][
                [
                    "Storm",
                    "sid",
                    "34 exp",
                    "34 fcast",
                    "34 rain",
                    "34+O",
                    "50 exp",
                    "50 fcast",
                    "50 rain",
                    "50+O",
                    "64 exp",
                    "64 fcast",
                    "64 rain",
                    "64+O",
                    *_bool_hide,
                    "old_combined",
                    "Total Affected",
                    "CERF",
                    "Old act.",
                    "Old mob.",
                    "Old obsv.",
                ]
            ]
            .sort_values("Total Affected", ascending=False, na_position="last")
            .reset_index(drop=True)
        )

        def _sc(val):
            return (
                "background-color: #e8f5e9; color: #2e7d32"
                if val == "✓"
                else "color: #ddd"
            )

        def _sco(val):
            return (
                "background-color: #ffa040; color: white; font-weight: bold"
                if val == "✓"
                else "color: #ccc"
            )

        def _sch(val):
            return (
                "background-color: #fff0b3; color: #888; font-weight: normal"
                if val == "✓"
                else "color: #ccc"
            )

        def _sc_cerf(val):
            if isinstance(val, str) and val.startswith("$"):
                return "background-color: crimson; color: white; font-weight: bold"
            if val == "combined":
                return "background-color: crimson; color: white; font-weight: bold"
            if val == "—":
                return "background-color: #cce5ff; color: #555"
            return "color: #aaa"

        _styl_storms = (
            _storm_tbl.style.map(_sc, subset=_cond_cols)
            .map(_sco, subset=_comb_cols)
            .map(_sch, subset=["Old act.", "Old mob.", "Old obsv."])
            .map(_sc_cerf, subset=["CERF"])
            .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .hide(axis="columns", subset=_bool_hide + ["old_combined"])
            .hide(axis="index")
        )

        _rp_note = mo.md(
            f"**n = {_n} storms** — return period {_rp:.1f} yrs "
            f"({_n_yrs} seasons 2002–{int(_opt['season'].max())})"
        )
        mo.output.replace(
            mo.vstack(
                [
                    _rp_note,
                    mo.md("### Summary"),
                    mo.Html(_styl_sum.to_html()),
                    mo.md(
                        "### All options (best per wind level + trigger split)"
                    ),
                    mo.Html(_styl_opts.to_html()),
                    mo.md("### Storm conditions"),
                    mo.Html(_styl_storms.to_html()),
                ]
            )
        )

    # Aliases so appendix cells (rain_trigger_opt_or_shared etc.) still work
    for _wkt in [34, 50, 64]:
        df_rain_opt[f"fcast_exp_{_wkt}"] = df_rain_opt[f"total_exp_{_wkt}"]
    # Add backward-compat keys to rain_opt_thresh for appendix cells
    for _wkt in [34, 50, 64]:
        if _wkt in rain_opt_thresh:
            rain_opt_thresh[_wkt]["exp_f"] = rain_opt_thresh[_wkt][
                "exp_thresh"
            ]
            rain_opt_thresh[_wkt]["exp_o"] = rain_opt_thresh[_wkt][
                "exp_thresh"
            ]
    # Column name aliases expected by appendix cells
    df_rain_opt["Action"] = df_rain_opt["Old act."]
    df_rain_opt["Mob. trig."] = df_rain_opt["Old mob."]
    df_rain_opt["Obsv. trig."] = df_rain_opt["Old obsv."]
    df_rain_opt["obsv_trig"] = df_rain_opt["obsv_trig"].astype(bool)
    df_rain_opt["Old A|O"] = (
        df_rain_opt["action_trig_old"]
        | df_rain_opt["mob_trig"]
        | df_rain_opt["obsv_trig"]
    ).map({True: "✓", False: "—"})
    return df_rain_opt, rain_opt_thresh


@app.cell
def rain_scatter(df_rain_opt, mo, mpatches, pd, plt, rain_opt_thresh):
    mo.stop(not len(df_rain_opt) or not rain_opt_thresh)
    _fig, _axes = plt.subplots(1, 3, figsize=(15, 5), dpi=120)

    for _col_idx, _wkt in enumerate([34, 50, 64]):
        _t = rain_opt_thresh.get(_wkt, {})
        _e_thresh = _t.get("exp_thresh", 0)
        _ro = _t.get("r_obs")
        _ax = _axes[_col_idx]
        _xcol = f"total_exp_{_wkt}"
        _sub = df_rain_opt[
            (df_rain_opt[_xcol].fillna(0) > 0)
            & df_rain_opt["max_obs_rain"].notna()
        ].copy()

        _colors = [
            "crimson" if r["has_cerf"] else "#aaaaaa"
            for _, r in _sub.iterrows()
        ]
        _max_aff = df_rain_opt["Total Affected"].max()
        _sizes = [
            (
                max(20, (float(v) ** 0.5) * 500 / (_max_aff**0.5))
                if pd.notna(v) and v > 0
                else 20
            )
            for v in _sub["Total Affected"]
        ]

        _ax.scatter(
            _sub[_xcol],
            _sub["max_obs_rain"],
            c=_colors,
            s=_sizes,
            alpha=0.7,
            edgecolors="none",
            zorder=2,
        )
        for _, _row in _sub.iterrows():
            _trig = bool(_row.get(f"combined_{_wkt}", False))
            _ax.annotate(
                _row["Storm"],
                xy=(_row[_xcol], _row["max_obs_rain"]),
                ha="center",
                va="center",
                fontsize=6,
                fontweight="bold" if _trig else "normal",
                zorder=3,
            )
        if _e_thresh:
            _ax.axvline(
                _e_thresh,
                color="steelblue",
                linewidth=1,
                linestyle="--",
                alpha=0.7,
            )
        if _ro is not None:
            _ax.axhline(
                _ro, color="darkorange", linewidth=1, linestyle="--", alpha=0.7
            )
        _ax.set_xlabel(f"Total exposure ({_wkt} kt)")
        _ax.set_ylabel("Obs rain 2d (mm)" if _col_idx == 0 else "")
        _ax.set_title(f"{_wkt} kt — total exp vs obs rain")
        _ax.grid(True, alpha=0.25, linestyle="--")
        _ax.set_xlim(left=0)
        _ax.set_ylim(bottom=0)

    _legend_patches = [
        mpatches.Patch(color="crimson", label="CERF"),
        mpatches.Patch(color="#aaaaaa", label="No CERF"),
    ]
    _fig.legend(
        handles=_legend_patches,
        loc="upper center",
        ncol=2,
        fontsize=9,
        bbox_to_anchor=(0.5, 1.02),
    )
    _fig.suptitle(
        "Total exposure vs. observed rainfall — bubble size ∝ impact",
        fontsize=11,
        y=1.05,
    )
    plt.tight_layout()
    _fig
    return


@app.cell
def doc_hur_opt(mo):
    mo.md(
        """
    ## Hurricane Warning OR trigger

    A variant where any storm for which NHC issued a **Hurricane Warning for Haiti**
    is automatically included in the trigger set. Our optimized conditions (total
    exposure OR forecast rainfall OR observed rainfall) then fill the remaining slots
    to reach n = 12 total.

    Because ~5 storms already carry a Hurricane Warning, our conditions only need to
    activate ~7 additional storms. This typically requires higher thresholds than the
    primary optimization, since the mandatory hur-warning storms take away the "easiest"
    activations (the high-impact storms that would trigger under almost any threshold).

    In the storm conditions table, **Hur. Warn** shows the pre-seeded storms; the
    `{wkt}+O` column shows the combined (hurricane warning OR our conditions) result.
    """
    )
    return


@app.cell
def rain_trigger_opt_hur(
    df_action_trig,
    df_exp,
    df_nhc_alerts,
    df_obs_rain,
    df_old_trig,
    df_rain,
    df_total_exp,
    mo,
    pd,
):
    _n = 12  # same target

    # ── Build _opt (identical to rain_trigger_opt) ────────────────────────  # noqa: E501
    _exp_pivot = (
        df_exp.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="pop_exposed",
            aggfunc="first",
        )
        .rename(columns={34: "exp_34", 50: "exp_50", 64: "exp_64"})
        .reset_index()
    )
    for _c in ["exp_34", "exp_50", "exp_64"]:
        if _c not in _exp_pivot.columns:
            _exp_pivot[_c] = 0

    _texp_pivot = (
        df_total_exp.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="max_total_exposure",
            aggfunc="max",
        )
        .rename(
            columns={
                34: "total_exp_34",
                50: "total_exp_50",
                64: "total_exp_64",
            }
        )
        .reset_index()
    )
    for _c in ["total_exp_34", "total_exp_50", "total_exp_64"]:
        if _c not in _texp_pivot.columns:
            _texp_pivot[_c] = 0

    _meta = df_exp[["sid", "season", "name"]].drop_duplicates("sid")
    _opt = _meta.merge(_exp_pivot, on="sid", how="outer")
    _opt = _opt.merge(_texp_pivot, on="sid", how="outer")

    _opt["_name_key"] = _opt["name"].str.strip().str.upper()
    _trig_lkp = df_old_trig.copy()
    _trig_lkp["_name_key"] = _trig_lkp["name"].str.strip().str.upper()
    _opt = _opt.merge(
        _trig_lkp[
            [
                "_name_key",
                "season",
                "mob_trig",
                "obsv_trig",
                "Total Affected",
                "Amount in US$",
            ]
        ],
        on=["_name_key", "season"],
        how="left",
        suffixes=("", "_hist"),
    )
    for _hcol in ["Total Affected", "Amount in US$"]:
        _hc = f"{_hcol}_hist"
        if _hc in _opt.columns:
            _opt[_hcol] = _opt[_hc].combine_first(_opt[_hcol])
            _opt = _opt.drop(columns=[_hc])

    # Identify Hurricane Warning storms
    _hw = df_nhc_alerts[df_nhc_alerts["hur_warning"]].copy()
    _hw["_name_key"] = _hw["name"].str.upper()
    _hw_df = _opt.merge(
        _hw[["_name_key", "season"]], on=["_name_key", "season"], how="inner"
    )
    _hur_sids = set(_hw_df["sid"].dropna())

    _opt = _opt.drop(columns=["_name_key"])
    _opt = _opt.merge(
        df_action_trig[["sid", "action_trig_old"]], on="sid", how="left"
    )

    _fcast_rain = (
        df_rain[df_rain["lt_name"] == "action"]
        .groupby("sid")["max_rain"]
        .max()
        .reset_index()
        .rename(columns={"max_rain": "max_fcast_rain"})
    )
    _opt = _opt.merge(_fcast_rain, on="sid", how="left")
    _opt = _opt.merge(
        df_obs_rain[["sid", "max_obs_rain"]], on="sid", how="left"
    )

    _opt = _opt.drop_duplicates("sid")
    for _c in [
        "exp_34",
        "exp_50",
        "exp_64",
        "total_exp_34",
        "total_exp_50",
        "total_exp_64",
    ]:
        _opt[_c] = _opt[_c].fillna(0)
    _opt["season"] = pd.to_numeric(
        _opt["season"].fillna(_opt["sid"].str[:4]), errors="coerce"
    ).astype("Int64")
    _opt["max_obs_rain"] = _opt["max_obs_rain"].fillna(0)
    _opt["has_cerf"] = _opt["Amount in US$"].notna() & (
        _opt["Amount in US$"] > 0
    )
    _opt["mob_trig"] = (
        _opt["mob_trig"].astype("boolean").fillna(False).astype(bool)
    )
    _opt["obsv_trig"] = (
        _opt["obsv_trig"].astype("boolean").fillna(False).astype(bool)
    )
    _opt["action_trig_old"] = (
        _opt["action_trig_old"].astype("boolean").fillna(False).astype(bool)
    )
    _opt["hur_warning"] = _opt["sid"].isin(_hur_sids)

    def _slbl(row):
        if pd.notna(row["name"]):
            _nm = str(row["name"]).strip().title()
        elif str(row.get("sid", "")) == "2002265N10315":
            _nm = "Lili"  # 2002 hurricane, missing from EM-DAT for Haiti
        else:
            _nm = "Unnamed"
        _yr = row["season"] if pd.notna(row["season"]) else row["sid"][:4]
        return f"{_nm} ({_yr})"

    _opt["Storm"] = _opt.apply(_slbl, axis=1)
    _opt["old_combined"] = _opt["mob_trig"] | _opt["obsv_trig"]
    _opt["_name_key"] = _opt["name"].str.strip().str.upper()

    _cerf_lkp = dict(zip(_opt["sid"], _opt["has_cerf"]))
    _aff_lkp = dict(zip(_opt["sid"], _opt["Total Affected"].fillna(0)))

    # ── Sweep: Hurricane Warning pre-seeded ───────────────────────────────  # noqa: E501
    _results = []
    for _wkt, _tcol in [
        (34, "total_exp_34"),
        (50, "total_exp_50"),
        (64, "total_exp_64"),
    ]:
        _exp_vals = sorted(_opt[_tcol].dropna().unique())
        _fcast_vals = sorted(
            _opt["max_fcast_rain"].dropna().unique(), reverse=True
        )
        for _e_thresh in _exp_vals:
            _our_exp = set(_opt[_opt[_tcol] >= _e_thresh]["sid"]) - _hur_sids
            for _r_fcast in _fcast_vals:
                _our_fcast = (
                    set(
                        _opt[
                            _opt["max_fcast_rain"].notna()
                            & (_opt["max_fcast_rain"] >= _r_fcast)
                        ]["sid"]
                    )
                    - _hur_sids
                )
                _our_new = _our_exp | _our_fcast
                _already = _hur_sids | _our_new
                _n_obs = _n - len(_already)
                if _n_obs < 0:
                    continue
                _remaining = _opt[~_opt["sid"].isin(_already)]
                if _n_obs == 0:
                    _combined = frozenset(_already)
                    _results.append(
                        {
                            "wkt": _wkt,
                            "exp_thresh": _e_thresh,
                            "r_fcast_exact": _r_fcast,
                            "r_fcast": round(_r_fcast, 1),
                            "r_obs_exact": None,
                            "r_obs": None,
                            "n_hur": len(_hur_sids),
                            "n_exp": len(_our_exp - _our_fcast),
                            "n_fcast": len(_our_fcast - _our_exp),
                            "n_obs": 0,
                            "cerf_count": sum(
                                1
                                for _s in _combined
                                if _cerf_lkp.get(_s, False)
                            ),
                            "total_affected": sum(
                                _aff_lkp.get(_s, 0) for _s in _combined
                            ),
                            "_combined_sids": _combined,
                            "_our_sids": frozenset(_our_new),
                        }
                    )
                else:
                    _s = _remaining[
                        _remaining["max_obs_rain"].notna()
                    ].sort_values("max_obs_rain", ascending=False)
                    if len(_s) < _n_obs:
                        continue
                    _r_obs = float(_s.iloc[_n_obs - 1]["max_obs_rain"])
                    _obs_sids = set(
                        _remaining[
                            _remaining["max_obs_rain"].notna()
                            & (_remaining["max_obs_rain"] >= _r_obs)
                        ]["sid"]
                    )
                    if len(_obs_sids) != _n_obs:
                        continue
                    _combined = frozenset(_already | _obs_sids)
                    if len(_combined) != _n:
                        continue
                    _results.append(
                        {
                            "wkt": _wkt,
                            "exp_thresh": _e_thresh,
                            "r_fcast_exact": _r_fcast,
                            "r_fcast": round(_r_fcast, 1),
                            "r_obs_exact": _r_obs,
                            "r_obs": round(_r_obs, 1),
                            "n_hur": len(_hur_sids),
                            "n_exp": len(_our_exp - _our_fcast),
                            "n_fcast": len(_our_fcast - _our_exp),
                            "n_obs": _n_obs,
                            "cerf_count": sum(
                                1
                                for _s in _combined
                                if _cerf_lkp.get(_s, False)
                            ),
                            "total_affected": sum(
                                _aff_lkp.get(_s, 0) for _s in _combined
                            ),
                            "_combined_sids": _combined,
                            "_our_sids": frozenset(_our_new),
                        }
                    )

    df_rain_opt_hur = _opt.copy()
    rain_opt_thresh_hur = {}

    if not _results:
        mo.output.replace(mo.md("⚠ No valid combinations found."))
    else:
        _df_res = pd.DataFrame(_results)
        _df_best = (
            _df_res.sort_values(
                ["wkt", "cerf_count", "total_affected", "exp_thresh"],
                ascending=[True, False, False, True],
            )
            .groupby("wkt", sort=True)
            .first()
            .reset_index()
        )

        for _, _brow in _df_best.iterrows():
            _wkt = int(_brow["wkt"])
            _opt[f"our_trig_{_wkt}"] = _opt["sid"].isin(_brow["_our_sids"])
            _opt[f"combined_{_wkt}"] = _opt["sid"].isin(
                _brow["_combined_sids"]
            )
        for _wkt in [34, 50, 64]:
            if f"our_trig_{_wkt}" not in _opt.columns:
                _opt[f"our_trig_{_wkt}"] = False
            if f"combined_{_wkt}" not in _opt.columns:
                _opt[f"combined_{_wkt}"] = False

        _best_thresh = {}
        for _, _r in _df_best.iterrows():
            _wkt = int(_r["wkt"])
            _best_thresh[_wkt] = {
                "exp_thresh": int(_r["exp_thresh"]),
                "r_fcast": float(_r["r_fcast_exact"])
                if pd.notna(_r["r_fcast_exact"])
                else None,
                "r_obs": float(_r["r_obs_exact"])
                if pd.notna(_r["r_obs_exact"])
                else None,
            }
        rain_opt_thresh_hur = _best_thresh

        # Condition flags
        _bool_hide = []
        for _wkt, _tcol in [
            (34, "total_exp_34"),
            (50, "total_exp_50"),
            (64, "total_exp_64"),
        ]:
            _t = _best_thresh.get(_wkt, {})
            _et, _rf, _ro = (
                _t.get("exp_thresh", float("inf")),
                _t.get("r_fcast"),
                _t.get("r_obs"),
            )
            _opt[f"_exp_flag_{_wkt}"] = _opt[_tcol] >= _et
            _opt[f"_fcast_flag_{_wkt}"] = (
                _opt["max_fcast_rain"].notna()
                & (_opt["max_fcast_rain"] >= _rf)
                if _rf is not None
                else pd.Series(False, index=_opt.index)
            )
            _opt[f"_rain_flag_{_wkt}"] = (
                _opt["max_obs_rain"] >= _ro
                if _ro is not None
                else pd.Series(False, index=_opt.index)
            )
            _opt[f"{_wkt} Hw"] = _opt["hur_warning"].map(
                {True: "✓", False: "—"}
            )
            _opt[f"{_wkt} exp"] = _opt[f"_exp_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _opt[f"{_wkt} fcast"] = _opt[f"_fcast_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _opt[f"{_wkt} rain"] = _opt[f"_rain_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _opt[f"{_wkt}+O"] = _opt[f"combined_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _bool_hide += [
                f"_exp_flag_{_wkt}",
                f"_fcast_flag_{_wkt}",
                f"_rain_flag_{_wkt}",
                f"our_trig_{_wkt}",
                f"combined_{_wkt}",
            ]

        df_rain_opt_hur = _opt

        # Summary
        _n_yrs = int(_opt["season"].max() - _opt["season"].min() + 1)
        _rp = (_n_yrs + 1) / _n
        _summary_rows = []
        for _, _r in _df_best.iterrows():
            _wb = int(_r["wkt"])
            _summary_rows.append(
                {
                    "Trigger": f"New {_wb} kt ★",
                    "Wind kt": f"{_wb}",
                    "Pre (Hur. Warn)": int(_r["n_hur"]),
                    "Exp thresh": f"{int(_r['exp_thresh']):,}",
                    "Fcast rain mm": f"{_r['r_fcast']:.1f}"
                    if pd.notna(_r["r_fcast"])
                    else "—",
                    "Obs rain mm": f"{_r['r_obs']:.1f}"
                    if pd.notna(_r["r_obs"])
                    else "—",
                    "# Our add": int(_r["n_exp"])
                    + int(_r["n_fcast"])
                    + int(_r["n_obs"]),
                    "CERF": int(_r["cerf_count"]),
                    "Total Affected": int(_r["total_affected"]),
                    "RP yrs": round(_rp, 1),
                }
            )
        _df_sum = pd.DataFrame(_summary_rows)
        _styl_sum = (
            _df_sum.style.bar(
                subset=["Total Affected"], color="#b39ddb", vmin=0
            )
            .format(
                {
                    "Total Affected": lambda x: f"{int(x):,}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .set_properties(**{"text-align": "center"})
            .set_properties(subset=["Trigger"], **{"text-align": "left"})
            .set_table_styles(
                [{"selector": "th", "props": [("text-align", "center")]}]
            )
            .hide(axis="index")
        )

        # Storm conditions table
        def _cerf_s(row):
            if pd.notna(row.get("Amount in US$")) and row["Amount in US$"] > 0:
                return f"${row['Amount in US$']:,.0f}"
            if pd.notna(row.get("season")) and int(row["season"]) == 2008:
                return "combined"
            if pd.notna(row.get("season")) and int(row["season"]) >= 2006:
                return "—"
            return "pre-"

        _opt["CERF"] = _opt.apply(_cerf_s, axis=1)
        _opt["Old act."] = _opt["action_trig_old"].map({True: "✓", False: "—"})

        _hw_cols = [f"{w} Hw" for w in [34, 50, 64]]
        _cond_cols = [
            f"{w} {c}" for w in [34, 50, 64] for c in ["exp", "fcast", "rain"]
        ]
        _comb_cols = [f"{w}+O" for w in [34, 50, 64]]

        _any_trig = _opt[[f"combined_{w}" for w in [34, 50, 64]]].any(axis=1)
        _show = (
            _any_trig
            | _opt["hur_warning"]
            | _opt["mob_trig"]
            | _opt["obsv_trig"]
            | _opt["action_trig_old"]
            | (_opt["Total Affected"].fillna(0) > 0)
            | _opt["has_cerf"]
        )
        _storm_tbl = (
            _opt[_show][
                [
                    "Storm",
                    "34 Hw",
                    "34 exp",
                    "34 fcast",
                    "34 rain",
                    "34+O",
                    "50 Hw",
                    "50 exp",
                    "50 fcast",
                    "50 rain",
                    "50+O",
                    "64 Hw",
                    "64 exp",
                    "64 fcast",
                    "64 rain",
                    "64+O",
                    *_bool_hide,
                    "old_combined",
                    "Total Affected",
                    "CERF",
                    "Old act.",
                ]
            ]
            .sort_values("Total Affected", ascending=False, na_position="last")
            .reset_index(drop=True)
        )

        def _sc(val):
            return (
                "background-color: #e8f5e9; color: #2e7d32"
                if val == "✓"
                else "color: #ddd"
            )

        def _shw(val):
            return (
                "background-color: crimson; color: white; font-weight: bold"
                if val == "✓"
                else "color: #ccc"
            )

        def _sco(val):
            return (
                "background-color: #ffa040; color: white; font-weight: bold"
                if val == "✓"
                else "color: #ccc"
            )

        def _sch(val):
            return (
                "background-color: #fff0b3; color: #888; font-weight: normal"
                if val == "✓"
                else "color: #ccc"
            )

        def _sc_cerf(val):
            if isinstance(val, str) and val.startswith("$"):
                return "background-color: crimson; color: white; font-weight: bold"
            if val == "combined":
                return "background-color: crimson; color: white; font-weight: bold"
            if val == "—":
                return "background-color: #cce5ff; color: #555"
            return "color: #aaa"

        _styl_storms = (
            _storm_tbl.style.map(_shw, subset=_hw_cols)
            .map(_sc, subset=_cond_cols)
            .map(_sco, subset=_comb_cols)
            .map(_sch, subset=["Old act."])
            .map(_sc_cerf, subset=["CERF"])
            .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .hide(axis="columns", subset=_bool_hide + ["old_combined"])
            .hide(axis="index")
        )

        _rp_note = mo.md(
            f"**n = {_n} storms** — {len(_hur_sids)} pre-seeded (Hur. Warning) + "
            f"up to {_n - len(_hur_sids)} from our trigger. "
            f"Return period {_rp:.1f} yrs ({_n_yrs} seasons 2002–{int(_opt['season'].max())})"
        )
        mo.output.replace(
            mo.vstack(
                [
                    _rp_note,
                    mo.md("### Summary"),
                    mo.Html(_styl_sum.to_html()),
                    mo.md("### Storm conditions"),
                    mo.Html(_styl_storms.to_html()),
                ]
            )
        )
    return df_rain_opt_hur, rain_opt_thresh_hur


@app.cell
def rain_scatter_hur(
    df_rain_opt_hur,
    mo,
    mpatches,
    pd,
    plt,
    rain_opt_thresh_hur,
):
    mo.stop(not len(df_rain_opt_hur) or not rain_opt_thresh_hur)
    _fig, _axes = plt.subplots(1, 3, figsize=(15, 5), dpi=120)

    for _col_idx, _wkt in enumerate([34, 50, 64]):
        _t = rain_opt_thresh_hur.get(_wkt, {})
        _e_thresh = _t.get("exp_thresh", 0)
        _ro = _t.get("r_obs")
        _ax = _axes[_col_idx]
        _xcol = f"total_exp_{_wkt}"
        _sub = df_rain_opt_hur[
            (df_rain_opt_hur[_xcol].fillna(0) > 0)
            & df_rain_opt_hur["max_obs_rain"].notna()
        ].copy()

        _colors = [
            "crimson" if r["has_cerf"] else "#aaaaaa"
            for _, r in _sub.iterrows()
        ]
        _max_aff = df_rain_opt_hur["Total Affected"].max()
        _sizes = [
            (
                max(20, (float(v) ** 0.5) * 500 / (_max_aff**0.5))
                if pd.notna(v) and v > 0
                else 20
            )
            for v in _sub["Total Affected"]
        ]

        _ax.scatter(
            _sub[_xcol],
            _sub["max_obs_rain"],
            c=_colors,
            s=_sizes,
            alpha=0.7,
            edgecolors="none",
            zorder=2,
        )
        for _, _row in _sub.iterrows():
            _trig = bool(_row.get(f"combined_{_wkt}", False))
            _ax.annotate(
                _row["Storm"],
                xy=(_row[_xcol], _row["max_obs_rain"]),
                ha="center",
                va="center",
                fontsize=6,
                fontweight="bold" if _trig else "normal",
                zorder=3,
            )
        if _e_thresh:
            _ax.axvline(
                _e_thresh,
                color="steelblue",
                linewidth=1,
                linestyle="--",
                alpha=0.7,
            )
        if _ro is not None:
            _ax.axhline(
                _ro, color="darkorange", linewidth=1, linestyle="--", alpha=0.7
            )
        _ax.set_xlabel(f"Total exposure ({_wkt} kt)")
        _ax.set_ylabel("Obs rain 2d (mm)" if _col_idx == 0 else "")
        _ax.set_title(f"{_wkt} kt — Hur. Warn pre-seeded")
        _ax.grid(True, alpha=0.25, linestyle="--")
        _ax.set_xlim(left=0)
        _ax.set_ylim(bottom=0)

    _legend_patches = [
        mpatches.Patch(color="crimson", label="CERF"),
        mpatches.Patch(color="#aaaaaa", label="No CERF"),
    ]
    _fig.legend(
        handles=_legend_patches,
        loc="upper center",
        ncol=2,
        fontsize=9,
        bbox_to_anchor=(0.5, 1.02),
    )
    _fig.suptitle(
        "Total exposure vs. obs rainfall — Hurricane Warning pre-seeded",
        fontsize=11,
        y=1.05,
    )
    plt.tight_layout()
    _fig
    return


@app.cell
def trigger_leadtime(
    df_mon_all,
    df_rain_opt,
    mo,
    pd,
    rain_opt_thresh,
    stratus,
    text,
):
    mo.stop(not len(df_rain_opt) or not rain_opt_thresh)

    _pre_cut = (
        df_mon_all[~df_mon_all["past_cutoff"]][["sid", "issue_time"]]
        .drop_duplicates()
        .rename(columns={"issue_time": "issued_time"})
    )

    _all_sids = set()
    for _wkt_lt in [34, 50, 64]:
        _all_sids |= set(
            df_rain_opt.loc[df_rain_opt[f"combined_{_wkt_lt}"], "sid"].tolist()
        )

    _sid_ph = ", ".join(f"'{s}'" for s in _all_sids) if _all_sids else "''"
    _engine = stratus.get_engine(stage="dev")
    with _engine.connect() as _conn:
        _df_atcf = pd.read_sql(
            text(
                f"SELECT sid, atcf_id FROM storms.ibtracs_storms"
                f" WHERE sid IN ({_sid_ph})"
            ),
            _conn,
        )
        _atcf_ids = _df_atcf["atcf_id"].tolist()
        if _atcf_ids:
            _atcf_ph = ", ".join(f"'{a}'" for a in _atcf_ids)
            _df_fcast_lt = pd.read_sql(
                text(
                    f"""
                    SELECT atcf_id, issued_time, wind_speed_kt,
                           pop_exposed AS fcast_exp
                    FROM storms.nhc_tracks_fcastonly_exposure
                    WHERE atcf_id IN ({_atcf_ph})
                      AND iso3 = 'HTI' AND admin_level = 0
                    ORDER BY atcf_id, issued_time
                    """
                ),
                _conn,
            )
            _df_obsv_lt = pd.read_sql(
                text(
                    f"""
                    SELECT atcf_id, valid_time, wind_speed_kt,
                           pop_exposed AS obsv_exp
                    FROM storms.nhc_tracks_obsv_exposure
                    WHERE atcf_id IN ({_atcf_ph})
                      AND iso3 = 'HTI' AND admin_level = 0
                    ORDER BY atcf_id, valid_time
                    """
                ),
                _conn,
            )
        else:
            _df_fcast_lt = pd.DataFrame(
                columns=[
                    "atcf_id",
                    "issued_time",
                    "wind_speed_kt",
                    "fcast_exp",
                ]
            )
            _df_obsv_lt = pd.DataFrame(
                columns=["atcf_id", "valid_time", "wind_speed_kt", "obsv_exp"]
            )
    _engine.dispose()

    _df_fcast_lt = _df_fcast_lt.merge(_df_atcf, on="atcf_id", how="left")
    _df_obsv_lt = _df_obsv_lt.merge(_df_atcf, on="atcf_id", how="left")

    _rows_lt = []
    for _wkt_lt in [34, 50, 64]:
        _t_lt = rain_opt_thresh.get(_wkt_lt, {})
        _et_lt = _t_lt.get("exp_thresh", float("inf"))
        _rf_lt = _t_lt.get("r_fcast")
        _triggered_lt = df_rain_opt[df_rain_opt[f"combined_{_wkt_lt}"]].copy()
        for _, _storm_lt in _triggered_lt.iterrows():
            _sid_lt = _storm_lt["sid"]
            _lbl_lt = _storm_lt["Storm"]
            _fs = _df_fcast_lt[
                (_df_fcast_lt["sid"] == _sid_lt)
                & (_df_fcast_lt["wind_speed_kt"] == _wkt_lt)
            ].sort_values("issued_time")
            # Apply pre-cutoff filter
            _fs = _fs.merge(
                _pre_cut[_pre_cut["sid"] == _sid_lt],
                on=["sid", "issued_time"],
                how="inner",
            )
            _os = _df_obsv_lt[
                (_df_obsv_lt["sid"] == _sid_lt)
                & (_df_obsv_lt["wind_speed_kt"] == _wkt_lt)
            ].sort_values("valid_time")

            _trig_t = None
            _arr_t = None
            _lead = None

            if not _fs.empty:
                _fs = _fs.copy()
                if not _os.empty:
                    _os2 = _os.assign(_cm=lambda x: x["obsv_exp"].cummax())
                    _m = pd.merge_asof(
                        _fs[["issued_time", "fcast_exp"]],
                        _os2[["valid_time", "_cm"]].rename(
                            columns={"valid_time": "issued_time"}
                        ),
                        on="issued_time",
                        direction="backward",
                    )
                    _fs["total_exp"] = (
                        _m["fcast_exp"].values + _m["_cm"].fillna(0).values
                    )
                else:
                    _fs["total_exp"] = _fs["fcast_exp"]

                _fcast_cond = (
                    _fs["max_fcast_rain"].notna()
                    & (_fs["max_fcast_rain"] >= _rf_lt)
                    if _rf_lt is not None and "max_fcast_rain" in _fs.columns
                    else pd.Series(False, index=_fs.index)
                )
                _met = _fs[(_fs["total_exp"] >= _et_lt) | _fcast_cond]
                if not _met.empty:
                    _trig_t = _met["issued_time"].min()

            if not _os.empty and len(_os) > 1:
                _os_d = _os.copy()
                _os_d["_diff"] = _os_d["obsv_exp"].diff()
                _idx_max = _os_d["_diff"].idxmax()
                if pd.notna(_idx_max) and _os_d.loc[_idx_max, "_diff"] > 0:
                    _arr_t = _os_d.loc[_idx_max, "valid_time"]
            elif not _os.empty:
                _arr_t = _os.iloc[0]["valid_time"]

            if _trig_t is not None and _arr_t is not None:
                _delta = pd.Timestamp(_arr_t) - pd.Timestamp(_trig_t)
                _lead = _delta.total_seconds() / 3600

            _rows_lt.append(
                {
                    "Wind kt": _wkt_lt,
                    "Storm": _lbl_lt,
                    "Trigger issued_time": _trig_t,
                    "Arrival time (max Δ obs)": _arr_t,
                    "Lead time": _lead,
                }
            )

    _df_lt = pd.DataFrame(_rows_lt).sort_values(
        ["Wind kt", "Lead time"], ascending=[True, False], na_position="last"
    )

    def _fmt_t(x):
        if x is None or pd.isna(x):
            return "—"
        return pd.Timestamp(x).strftime("%Y-%m-%d %H:%M")

    def _fmt_lead(x):
        if x is None or pd.isna(x):
            return "no obs data"
        d, h = int(x) // 24, int(x) % 24
        return f"{d}d {h}h" if d else f"{h}h"

    _styl_lt = (
        _df_lt.style.format(
            {
                "Trigger issued_time": _fmt_t,
                "Arrival time (max Δ obs)": _fmt_t,
                "Lead time": _fmt_lead,
            }
        )
        .set_properties(**{"text-align": "center"})
        .set_properties(subset=["Storm"], **{"text-align": "left"})
        .set_table_styles(
            [{"selector": "th", "props": [("text-align", "center")]}]
        )
        .hide(axis="index")
    )
    mo.output.replace(
        mo.vstack(
            [
                mo.md(
                    "**Lead time analysis** — trigger fired before / after arrival. "
                    "Arrival = time of largest jump in observed exposure at each wind level"
                ),
                mo.Html(_styl_lt.to_html()),
            ]
        )
    )
    return


@app.cell
def doc_appendix(mo):
    mo.md(
        """
    ---
    ## Appendix — additional trigger variants

    The following cells preserve the previous optimization variants for reference
    and comparison. They use the same `df_rain_opt` base data from the primary
    optimization above.
    """
    )
    return


@app.cell
def rain_trigger_opt_or_shared(df_rain_opt, mo, pd):
    mo.stop(not len(df_rain_opt))
    _n = 12
    _base = df_rain_opt.copy()
    _opt = _base[
        _base["max_fcast_rain"].notna() | (_base["max_obs_rain"] > 0)
    ].copy()

    # OR sweep with single shared exposure threshold for both forecast and obs.
    # fcast fires if fcast_exp >= exp_thresh OR fcast_rain >= r_fcast
    # obs fires (independently) if obsv_exp >= exp_thresh OR obs_rain >= r_obs
    _results = []
    for _wkt, _fcol, _ocol in [
        (34, "fcast_exp_34", "exp_34"),
        (50, "fcast_exp_50", "exp_50"),
        (64, "fcast_exp_64", "exp_64"),
    ]:
        _exp_vals = sorted(_opt[_fcol].unique())
        _fcast_vals = sorted(
            _opt["max_fcast_rain"].dropna().unique(), reverse=True
        )
        for _e_thresh in _exp_vals:
            for _r_fcast in _fcast_vals:
                _fcast_mask = (_opt[_fcol] >= _e_thresh) | (
                    _opt["max_fcast_rain"].notna()
                    & (_opt["max_fcast_rain"] >= _r_fcast)
                )
                _pool_fcast = _opt[_fcast_mask]
                _n_f = len(_pool_fcast)
                if _n_f > _n:
                    continue
                _n_o = _n - _n_f
                _fcast_sids_set = set(_pool_fcast["sid"])
                _not_fcast = _opt[~_fcast_mask]

                if _n_o == 0:
                    _trig_rows = _base[_base["sid"].isin(_fcast_sids_set)]
                    _results.append(
                        {
                            "wind_kt": _wkt,
                            "exp_thresh": int(_e_thresh),
                            "r_fcast": round(_r_fcast, 1),
                            "r_obs": None,
                            "r_fcast_exact": _r_fcast,
                            "r_obs_exact": None,
                            "n_fcast": _n_f,
                            "n_obsv": 0,
                            "cerf_count": int(_trig_rows["has_cerf"].sum()),
                            "total_affected": int(
                                _trig_rows["Total Affected"].fillna(0).sum()
                            ),
                            "_combined_sids": frozenset(_fcast_sids_set),
                            "_fcast_sids": frozenset(_fcast_sids_set),
                        }
                    )
                else:
                    _auto_obs = _not_fcast[_not_fcast[_ocol] >= _e_thresh]
                    _n_auto = len(_auto_obs)
                    if _n_auto > _n_o:
                        continue
                    _n_rain_needed = _n_o - _n_auto
                    _rain_pool = _not_fcast[_not_fcast[_ocol] < _e_thresh]
                    if _n_rain_needed == 0:
                        _r_obs = None
                        _obs_new_sids = set(_auto_obs["sid"])
                    else:
                        if len(_rain_pool) < _n_rain_needed:
                            continue
                        _rp_s = _rain_pool.sort_values(
                            "max_obs_rain", ascending=False
                        )
                        _r_obs = float(
                            _rp_s.iloc[_n_rain_needed - 1]["max_obs_rain"]
                        )
                        _obs_rain_sids = set(
                            _rain_pool[_rain_pool["max_obs_rain"] >= _r_obs][
                                "sid"
                            ]
                        )
                        if len(_obs_rain_sids) != _n_rain_needed:
                            continue
                        _obs_new_sids = set(_auto_obs["sid"]) | _obs_rain_sids
                    _combined_sids = _fcast_sids_set | _obs_new_sids
                    if len(_combined_sids) != _n:
                        continue
                    _trig_rows = _base[_base["sid"].isin(_combined_sids)]
                    _results.append(
                        {
                            "wind_kt": _wkt,
                            "exp_thresh": int(_e_thresh),
                            "r_fcast": round(_r_fcast, 1),
                            "r_obs": round(_r_obs, 1)
                            if _r_obs is not None
                            else None,
                            "r_fcast_exact": _r_fcast,
                            "r_obs_exact": _r_obs,
                            "n_fcast": _n_f,
                            "n_obsv": _n_o,
                            "cerf_count": int(_trig_rows["has_cerf"].sum()),
                            "total_affected": int(
                                _trig_rows["Total Affected"].fillna(0).sum()
                            ),
                            "_combined_sids": frozenset(_combined_sids),
                            "_fcast_sids": frozenset(_fcast_sids_set),
                        }
                    )

    df_rain_opt_or_s = _base
    rain_opt_thresh_or_s = {}

    if not _results:
        mo.output.replace(
            mo.md("⚠ No valid combinations found (OR shared threshold).")
        )
    else:
        _df_opt = pd.DataFrame(_results)

        _df_options = (
            _df_opt.sort_values(
                [
                    "wind_kt",
                    "n_fcast",
                    "cerf_count",
                    "total_affected",
                    "r_fcast",
                ],
                ascending=[True, True, False, False, True],
            )
            .groupby(["wind_kt", "n_fcast"], sort=True)
            .first()
            .reset_index()
        )
        _df_best = (
            _df_opt.sort_values(
                ["wind_kt", "cerf_count", "total_affected", "r_fcast"],
                ascending=[True, False, False, True],
            )
            .groupby("wind_kt", sort=True)
            .first()
            .reset_index()
        )

        _best_keys = set(zip(_df_best["wind_kt"], _df_best["n_fcast"]))
        _df_options["best"] = [
            (r["wind_kt"], r["n_fcast"]) in _best_keys
            for _, r in _df_options.iterrows()
        ]

        for _, _brow in _df_best.iterrows():
            _wkt = int(_brow["wind_kt"])
            _base[f"fcast_trig_{_wkt}"] = _base["sid"].isin(
                _brow["_fcast_sids"]
            )
            _base[f"combined_{_wkt}"] = _base["sid"].isin(
                _brow["_combined_sids"]
            )

        for _wkt in [34, 50, 64]:
            if f"fcast_trig_{_wkt}" not in _base.columns:
                _base[f"fcast_trig_{_wkt}"] = False
            if f"combined_{_wkt}" not in _base.columns:
                _base[f"combined_{_wkt}"] = False

        _best_thresh = {}
        for _, _r in _df_best.iterrows():
            _wkt = int(_r["wind_kt"])
            _best_thresh[_wkt] = {
                "exp": int(_r["exp_thresh"]),
                "r_fcast": float(_r["r_fcast_exact"]),
                "r_obs": float(_r["r_obs_exact"])
                if pd.notna(_r["r_obs_exact"])
                else None,
            }
        rain_opt_thresh_or_s = _best_thresh

        _bool_hide = []
        for _wkt, _fcol, _ocol in [
            (34, "fcast_exp_34", "exp_34"),
            (50, "fcast_exp_50", "exp_50"),
            (64, "fcast_exp_64", "exp_64"),
        ]:
            _t = _best_thresh.get(_wkt, {})
            _e = _t.get("exp", float("inf"))
            _rf = _t.get("r_fcast", float("inf"))
            _ro = _t.get("r_obs")
            _base[f"_fexp_flag_{_wkt}"] = _base[_fcol] >= _e
            _base[f"_oexp_flag_{_wkt}"] = _base[_ocol] >= _e
            _base[f"_fcast_rf_{_wkt}"] = _base["max_fcast_rain"].notna() & (
                _base["max_fcast_rain"] >= _rf
            )
            _base[f"_obs_rf_{_wkt}"] = (
                (_base["max_obs_rain"] >= _ro)
                if _ro is not None
                else pd.Series(False, index=_base.index)
            )
            _base[f"{_wkt} fexp"] = _base[f"_fexp_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} fcast"] = _base[f"_fcast_rf_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} oexp"] = _base[f"_oexp_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} obs"] = _base[f"_obs_rf_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} kt"] = _base[f"fcast_trig_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt}+O"] = _base[f"combined_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _bool_hide += [
                f"_fexp_flag_{_wkt}",
                f"_fcast_rf_{_wkt}",
                f"_oexp_flag_{_wkt}",
                f"_obs_rf_{_wkt}",
                f"fcast_trig_{_wkt}",
                f"combined_{_wkt}",
            ]

        df_rain_opt_or_s = _base

        _storm_table = (
            _base[
                [
                    "Storm",
                    "34 fexp",
                    "34 fcast",
                    "34 oexp",
                    "34 obs",
                    "34 kt",
                    "34+O",
                    "50 fexp",
                    "50 fcast",
                    "50 oexp",
                    "50 obs",
                    "50 kt",
                    "50+O",
                    "64 fexp",
                    "64 fcast",
                    "64 oexp",
                    "64 obs",
                    "64 kt",
                    "64+O",
                    *_bool_hide,
                    "Old A|O",
                    "old_combined",
                    "Total Affected",
                    "CERF",
                    "Action",
                    "Mob. trig.",
                    "Obsv. trig.",
                ]
            ]
            .sort_values("Total Affected", ascending=False, na_position="last")
            .reset_index(drop=True)
        )

        _cond_cols = [
            f"{w} {c}"
            for w in [34, 50, 64]
            for c in ["fexp", "fcast", "oexp", "obs"]
        ]
        _trig_cols = [f"{w} kt" for w in [34, 50, 64]]
        _comb_cols = [f"{w}+O" for w in [34, 50, 64]]

        def _sc(val):
            return (
                "background-color: #e8f5e9; color: #2e7d32"
                if val == "✓"
                else "color: #ddd"
            )

        def _st(val):
            return (
                "background-color: gold; font-weight: bold"
                if val == "✓"
                else "color: #ccc"
            )

        def _sco(val):
            return (
                "background-color: #ffa040; color: white; font-weight: bold"
                if val == "✓"
                else "color: #ccc"
            )

        def _sch(val):
            return (
                "background-color: #fff0b3; color: #888; font-weight: normal"
                if val == "✓"
                else "color: #ccc"
            )

        def _soc(val):
            return (
                "background-color: #9c27b0; color: white; font-weight: bold"
                if val == "✓"
                else "color: #ccc"
            )

        def _scerf(val):
            if isinstance(val, str) and val.startswith("$"):
                return "background-color: crimson; color: white; font-weight: bold"
            if val == "combined":
                return "background-color: crimson; color: white; font-weight: bold; opacity: 0.6"
            if val == "—":
                return "background-color: #cce5ff; color: #555"
            return "color: #aaa"

        _styled_storms = (
            _storm_table.style.map(_sc, subset=_cond_cols)
            .map(_st, subset=_trig_cols)
            .map(_sco, subset=_comb_cols)
            .map(_soc, subset=["Old A|O"])
            .map(_sch, subset=["Action", "Mob. trig.", "Obsv. trig."])
            .map(_scerf, subset=["CERF"])
            .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .hide(axis="columns", subset=_bool_hide + ["old_combined"])
            .hide(axis="index")
        )

        _n_yrs = int(_base["season"].max() - _base["season"].min() + 1)
        _rp = (_n_yrs + 1) / _n

        _old_sub = _base[_base["old_combined"]]
        _n_old = len(_old_sub)
        _cerf_old = int(_old_sub["has_cerf"].sum())
        _aff_old = int(_old_sub["Total Affected"].fillna(0).sum())
        _rp_old = (_n_yrs + 1) / _n_old if _n_old else float("inf")
        _n_old_fcast = int(_base["action_trig_old"].sum())
        _n_old_obsv_add = int(
            (~_base["action_trig_old"] & _base["obsv_trig"]).sum()
        )

        _summary_rows = [
            {
                "Trigger": "Old (action|obsv)",
                "Wind": "≥64 kt speed",
                "Rain fcast mm": "≥42",
                "Rain obsv mm": "≥70",
                "# Fcast": _n_old_fcast,
                "# Obsv": _n_old_obsv_add,
                "n": _n_old,
                "CERF": _cerf_old,
                "Total Affected": _aff_old,
                "RP yrs": round(_rp_old, 1),
            }
        ]
        for _, _r in _df_best.iterrows():
            _wkt = int(_r["wind_kt"])
            _summary_rows.append(
                {
                    "Trigger": f"OR-S {_wkt} kt ★",
                    "Wind": f"≥{int(_r['exp_thresh']):,} exp",
                    "Rain fcast mm": f"≥{_r['r_fcast']}",
                    "Rain obsv mm": f"≥{_r['r_obs']}"
                    if pd.notna(_r["r_obs"])
                    else "—",
                    "# Fcast": int(_r["n_fcast"]),
                    "# Obsv": int(_r["n_obsv"]),
                    "n": _n,
                    "CERF": int(_r["cerf_count"]),
                    "Total Affected": int(_r["total_affected"]),
                    "RP yrs": round(_rp, 1),
                }
            )
        _df_summary = pd.DataFrame(_summary_rows)

        def _style_summary_row(row):
            _bg = (
                "background-color: #f3e5f5"
                if row["Trigger"] == "Old (action|obsv)"
                else ""
            )
            return [_bg] * len(row)

        _styled_summary = (
            _df_summary.style.apply(_style_summary_row, axis=1)
            .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if isinstance(x, (int, float))
                    else x
                }
            )
            .set_properties(**{"text-align": "center"})
            .set_properties(
                subset=["Trigger", "Wind"], **{"text-align": "left"}
            )
            .set_table_styles(
                [{"selector": "th", "props": [("text-align", "center")]}]
            )
            .hide(axis="index")
        )

        _opt_disp = _df_options[
            [
                "wind_kt",
                "n_fcast",
                "n_obsv",
                "exp_thresh",
                "r_fcast",
                "r_obs",
                "cerf_count",
                "total_affected",
                "best",
            ]
        ].copy()
        _opt_disp["r_obs"] = _opt_disp["r_obs"].apply(
            lambda x: str(x) if pd.notna(x) else "—"
        )
        _opt_disp["best"] = _opt_disp["best"].map({True: "★", False: ""})
        _opt_disp = _opt_disp.rename(
            columns={
                "wind_kt": "Wind kt",
                "n_fcast": "# Fcast",
                "n_obsv": "# Obsv",
                "exp_thresh": "Exp thresh",
                "r_fcast": "Rain fcast mm",
                "r_obs": "Rain obsv mm",
                "cerf_count": "CERF #",
                "total_affected": "Total Affected",
                "best": " ",
            }
        )

        def _sbest(val):
            return (
                "background-color: gold; font-weight: bold"
                if val == "★"
                else ""
            )

        _styled_opts = (
            _opt_disp.style.bar(
                subset=["Total Affected"], color="#b39ddb", vmin=0
            )
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .map(_sbest, subset=[" "])
            .hide(axis="index")
        )

        mo.output.replace(
            mo.vstack(
                [
                    mo.md(
                        f"### OR trigger — shared exposure threshold, n={_n}  \n"
                        f"Forecast: **fcast_exp ≥ thresh OR rain ≥ r_fcast**; "
                        f"Obsv: **obsv_exp ≥ thresh OR rain ≥ r_obs** (same thresh for both).  \n"
                        f"★ = best overall per wind level."
                    ),
                    mo.md("**Summary vs. old trigger:**"),
                    mo.Html(_styled_summary.to_html()),
                    mo.md(
                        "**All options per wind level × fcast/obsv split:**"
                    ),
                    mo.Html(_styled_opts.to_html()),
                    mo.md("**Storm table:**"),
                    mo.Html(_styled_storms.to_html()),
                ]
            )
        )
    return df_rain_opt_or_s, rain_opt_thresh_or_s


@app.cell
def rain_scatter_or_shared(
    df_rain_opt_or_s,
    mo,
    pd,
    plt,
    rain_opt_thresh_or_s,
):
    mo.stop(not rain_opt_thresh_or_s)

    _WKTS = [34, 50, 64]
    _max_aff = df_rain_opt_or_s["Total Affected"].fillna(0).max()

    def _bsz(val):
        return max(20, (val / max(_max_aff, 1)) ** 0.5 * 500)

    def _label(row):
        s = row.get("Storm", "")
        return s.split("(")[0].strip() if pd.notna(s) and s else ""

    _fig, _axes = plt.subplots(2, 3, figsize=(18, 10), dpi=120)
    _fig.suptitle(
        "OR trigger (shared threshold) — exposure vs. rainfall (bubble = impact)",
        fontsize=12,
    )

    for _ci, _wkt in enumerate(_WKTS):
        _t = rain_opt_thresh_or_s[_wkt]
        _fcol = f"fcast_exp_{_wkt}"
        _ocol = f"exp_{_wkt}"
        _e_thresh = _t["exp"]
        _r_fcast = _t["r_fcast"]
        _r_obs = _t.get("r_obs")

        # ── Forecast rain (row 0) ──────────────────────────────────────────
        _ax = _axes[0, _ci]
        _df = df_rain_opt_or_s[
            (df_rain_opt_or_s[_fcol].fillna(0) > 0)
            & df_rain_opt_or_s["max_fcast_rain"].notna()
        ].copy()
        _df["_s"] = _df["Total Affected"].fillna(0).apply(_bsz)
        _df["_c"] = "#cccccc"
        _df.loc[_df[f"fcast_trig_{_wkt}"], "_c"] = "gold"
        _df.loc[_df["has_cerf"], "_c"] = "crimson"
        _ax.scatter(
            _df[_fcol],
            _df["max_fcast_rain"],
            s=_df["_s"],
            c=_df["_c"],
            edgecolors="#888",
            linewidths=0.5,
            alpha=0.85,
            zorder=3,
        )
        for _, _row in _df.iterrows():
            _ax.annotate(
                _label(_row),
                xy=(_row[_fcol], _row["max_fcast_rain"]),
                fontsize=6.5,
                ha="center",
                va="bottom",
                color="#333",
            )
        _ax.axvline(
            _e_thresh,
            color="goldenrod",
            linestyle="--",
            linewidth=1.2,
            zorder=4,
        )
        _ax.axhline(
            _r_fcast,
            color="goldenrod",
            linestyle="--",
            linewidth=1.2,
            zorder=4,
        )
        _ax.set_xlabel(f"Forecast pop. exposed ({_wkt} kt)")
        _ax.set_ylabel("Forecast rain (mm)" if _ci == 0 else "")
        _ax.set_title(f"{_wkt} kt — forecast rain (OR-S)")
        _ax.grid(True, alpha=0.25, linestyle="--")
        _ax.set_xlim(left=0)
        _ax.set_ylim(bottom=0)

        # ── Observed rain (row 1) ──────────────────────────────────────────
        _ax = _axes[1, _ci]
        _df = df_rain_opt_or_s[df_rain_opt_or_s[_ocol].fillna(0) > 0].copy()
        _df["_s"] = _df["Total Affected"].fillna(0).apply(_bsz)
        _obs_hit = _df[f"_oexp_flag_{_wkt}"] | _df[f"_obs_rf_{_wkt}"]
        _df["_c"] = "#cccccc"
        _df.loc[_obs_hit, "_c"] = "#ffa040"
        _df.loc[_df["has_cerf"], "_c"] = "crimson"
        _ax.scatter(
            _df[_ocol],
            _df["max_obs_rain"],
            s=_df["_s"],
            c=_df["_c"],
            edgecolors="#888",
            linewidths=0.5,
            alpha=0.85,
            zorder=3,
        )
        for _, _row in _df.iterrows():
            _ax.annotate(
                _label(_row),
                xy=(_row[_ocol], _row["max_obs_rain"]),
                fontsize=6.5,
                ha="center",
                va="bottom",
                color="#333",
            )
        _ax.axvline(
            _e_thresh, color="#ff8c00", linestyle="--", linewidth=1.2, zorder=4
        )
        if _r_obs is not None:
            _ax.axhline(
                _r_obs,
                color="#ff8c00",
                linestyle="--",
                linewidth=1.2,
                zorder=4,
            )
        _ax.set_xlabel(f"Observed pop. exposed ({_wkt} kt)")
        _ax.set_ylabel("Observed rain (mm)" if _ci == 0 else "")
        _ax.set_title(f"{_wkt} kt — observed rain (OR-S)")
        _ax.grid(True, alpha=0.25, linestyle="--")
        _ax.set_xlim(left=0)
        _ax.set_ylim(bottom=0)

    plt.tight_layout()
    _fig
    return


@app.cell
def rain_trigger_opt_and(
    df_action_trig,
    df_exp,
    df_obs_rain,
    df_old_trig,
    df_rain,
    df_total_exp,
    mo,
    pd,
):
    _n = 12

    _rain_action = df_rain[df_rain["lt_name"] == "action"][
        ["sid", "max_rain"]
    ].rename(columns={"max_rain": "max_fcast_rain"})

    _exp_pivot = (
        df_exp.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="pop_exposed",
            aggfunc="first",
        )
        .rename(columns={34: "exp_34", 50: "exp_50", 64: "exp_64"})
        .reset_index()
    )
    for _c in ["exp_34", "exp_50", "exp_64"]:
        if _c not in _exp_pivot.columns:
            _exp_pivot[_c] = 0

    _meta = df_exp[["sid", "season", "name"]].drop_duplicates("sid")
    _trig_lu = df_old_trig.copy()
    _trig_lu["_name_key"] = _trig_lu["name"].str.strip().str.upper()

    _meta_keyed = _meta.copy()
    _meta_keyed["_name_key"] = _meta_keyed["name"].str.strip().str.upper()
    _hist_sids = _meta_keyed.merge(
        _trig_lu[["_name_key", "season"]],
        on=["_name_key", "season"],
        how="inner",
    )[["sid"]].drop_duplicates()

    _all_sids = pd.concat(
        [_rain_action[["sid"]], _hist_sids], ignore_index=True
    ).drop_duplicates("sid")

    # max_fcast_rain stays NaN for non-monitor storms — they can only be obs-triggered
    _base = _all_sids.merge(_rain_action, on="sid", how="left")
    _base = _base.merge(_meta, on="sid", how="left")
    _base = _base.merge(_exp_pivot, on="sid", how="left")
    _base["season"] = pd.to_numeric(
        _base["season"].fillna(_base["sid"].str[:4]), errors="coerce"
    ).astype("Int64")
    for _c in ["exp_34", "exp_50", "exp_64"]:
        _base[_c] = _base[_c].fillna(0)

    # Forecast exposure (max pre-cutoff forecast exposure)
    _fcast_exp_pivot = (
        df_total_exp.pivot_table(
            index="sid",
            columns="wind_speed_kt",
            values="max_total_exposure",
            aggfunc="max",
        )
        .rename(
            columns={
                34: "fcast_exp_34",
                50: "fcast_exp_50",
                64: "fcast_exp_64",
            }
        )
        .reset_index()
    )
    for _c in ["fcast_exp_34", "fcast_exp_50", "fcast_exp_64"]:
        if _c not in _fcast_exp_pivot.columns:
            _fcast_exp_pivot[_c] = 0
    _base = _base.merge(_fcast_exp_pivot, on="sid", how="left")
    for _c in ["fcast_exp_34", "fcast_exp_50", "fcast_exp_64"]:
        _base[_c] = _base[_c].fillna(0)

    _base = _base.merge(
        df_obs_rain[["sid", "max_obs_rain"]], on="sid", how="left"
    )
    _base["max_obs_rain"] = _base["max_obs_rain"].fillna(0)

    _base["_name_key"] = _base["name"].str.strip().str.upper()
    _base = _base.merge(
        _trig_lu[
            [
                "_name_key",
                "season",
                "mob_trig",
                "obsv_trig",
                "Total Affected",
                "Amount in US$",
            ]
        ],
        on=["_name_key", "season"],
        how="left",
    ).drop(columns=["_name_key"])

    _base = _base.merge(
        df_action_trig[["sid", "action_trig_old"]], on="sid", how="left"
    )
    _base["mob_trig"] = (
        _base["mob_trig"].astype("boolean").fillna(False).astype(bool)
    )
    _base["obsv_trig"] = (
        _base["obsv_trig"].astype("boolean").fillna(False).astype(bool)
    )
    _base["action_trig_old"] = (
        _base["action_trig_old"].astype("boolean").fillna(False).astype(bool)
    )
    _base["has_cerf"] = (
        _base["Amount in US$"].notna() & (_base["Amount in US$"] > 0)
    ) | (
        (_base["season"] == 2008)
        & (_base["name"].str.upper().str.strip().ne("FAY"))
    )

    def _storm_label(row):
        _nm = (
            str(row["name"]).strip().title()
            if pd.notna(row["name"])
            else row["sid"]
        )
        return f"{_nm} ({row['season']})"

    def _cerf_str(row):
        if pd.notna(row["Amount in US$"]) and row["Amount in US$"] > 0:
            return f"${row['Amount in US$']:,.0f}"
        if pd.notna(row["season"]) and int(row["season"]) == 2008:
            return "combined"
        if pd.notna(row["season"]) and int(row["season"]) >= 2006:
            return "—"
        return "pre-"

    _base["Storm"] = _base.apply(_storm_label, axis=1)
    _base["CERF"] = _base.apply(_cerf_str, axis=1)
    _base["old_combined"] = _base["action_trig_old"] | _base["obsv_trig"]
    _base["Action"] = _base["action_trig_old"].map({True: "✓", False: "—"})
    _base["Mob. trig."] = _base["mob_trig"].map({True: "✓", False: "—"})
    _base["Obsv. trig."] = _base["obsv_trig"].map({True: "✓", False: "—"})
    _base["Old A|O"] = _base["old_combined"].map({True: "✓", False: "—"})

    # Optimization pool: storms with forecast or observed rain data
    _opt_sids = set(_rain_action["sid"]) | set(df_obs_rain["sid"])
    _opt = _base[_base["sid"].isin(_opt_sids)].copy()

    # Triple sweep (AND):
    # fcast trigger = fcast_exp >= exp_thresh_f AND fcast_rain >= r_fcast
    # obs trigger   = obsv_exp  >= exp_thresh_o AND obs_rain  >= r_obs
    # (separate exposure thresholds; same wind speed level)
    _results = []
    for _wkt, _fcol, _ocol in [
        (34, "fcast_exp_34", "exp_34"),
        (50, "fcast_exp_50", "exp_50"),
        (64, "fcast_exp_64", "exp_64"),
    ]:
        _exp_f_vals = sorted(_opt[_fcol].unique())
        _exp_o_vals = sorted(_opt[_ocol].unique())
        _fcast_vals = sorted(
            _opt["max_fcast_rain"].dropna().unique(), reverse=True
        )

        for _e_thresh_f in _exp_f_vals:
            for _r_fcast in _fcast_vals:
                _pool_fcast = _opt[
                    (_opt[_fcol] >= _e_thresh_f)
                    & _opt["max_fcast_rain"].notna()
                    & (_opt["max_fcast_rain"] >= _r_fcast)
                ]
                _n_f = len(_pool_fcast)
                if _n_f > _n:
                    continue
                _n_o = _n - _n_f
                _fcast_sids_set = set(_pool_fcast["sid"])
                _not_fcast = _opt[~_opt["sid"].isin(_fcast_sids_set)]

                if _n_o == 0:
                    _trig_rows = _base[_base["sid"].isin(_fcast_sids_set)]
                    _results.append(
                        {
                            "wind_kt": _wkt,
                            "exp_thresh_f": int(_e_thresh_f),
                            "exp_thresh_o": None,
                            "r_fcast": round(_r_fcast, 1),
                            "r_obs": None,
                            "r_fcast_exact": _r_fcast,
                            "r_obs_exact": None,
                            "n_fcast": _n_f,
                            "n_obsv": 0,
                            "cerf_count": int(_trig_rows["has_cerf"].sum()),
                            "total_affected": int(
                                _trig_rows["Total Affected"].fillna(0).sum()
                            ),
                            "_combined_sids": frozenset(_fcast_sids_set),
                            "_fcast_sids": frozenset(_fcast_sids_set),
                        }
                    )
                else:
                    for _e_thresh_o in _exp_o_vals:
                        _obs_eligible = _not_fcast[
                            _not_fcast[_ocol] >= _e_thresh_o
                        ]
                        if len(_obs_eligible) < _n_o:
                            continue
                        _oe_s = _obs_eligible.sort_values(
                            "max_obs_rain", ascending=False
                        )
                        _r_obs = float(_oe_s.iloc[_n_o - 1]["max_obs_rain"])
                        _obs_new_sids = set(
                            _obs_eligible[
                                _obs_eligible["max_obs_rain"] >= _r_obs
                            ]["sid"]
                        )
                        if len(_obs_new_sids) != _n_o:
                            continue
                        _combined_sids = _fcast_sids_set | _obs_new_sids
                        if len(_combined_sids) != _n:
                            continue
                        _trig_rows = _base[_base["sid"].isin(_combined_sids)]
                        _results.append(
                            {
                                "wind_kt": _wkt,
                                "exp_thresh_f": int(_e_thresh_f),
                                "exp_thresh_o": int(_e_thresh_o),
                                "r_fcast": round(_r_fcast, 1),
                                "r_obs": round(_r_obs, 1),
                                "r_fcast_exact": _r_fcast,
                                "r_obs_exact": _r_obs,
                                "n_fcast": _n_f,
                                "n_obsv": _n_o,
                                "cerf_count": int(
                                    _trig_rows["has_cerf"].sum()
                                ),
                                "total_affected": int(
                                    _trig_rows["Total Affected"]
                                    .fillna(0)
                                    .sum()
                                ),
                                "_combined_sids": frozenset(_combined_sids),
                                "_fcast_sids": frozenset(_fcast_sids_set),
                            }
                        )

    df_rain_opt_and = _base
    rain_opt_thresh_and = {}

    if not _results:
        mo.output.replace(
            mo.md("⚠ No valid threshold combinations found for this n target.")
        )
    else:
        _df_opt = pd.DataFrame(_results)

        # Best per (wkt, n_fcast): most CERF, then most affected, then lowest r_fcast
        _df_options = (
            _df_opt.sort_values(
                [
                    "wind_kt",
                    "n_fcast",
                    "cerf_count",
                    "total_affected",
                    "r_fcast",
                ],
                ascending=[True, True, False, False, True],
            )
            .groupby(["wind_kt", "n_fcast"], sort=True)
            .first()
            .reset_index()
        )

        # Best overall per wkt
        _df_best = (
            _df_opt.sort_values(
                ["wind_kt", "cerf_count", "total_affected", "r_fcast"],
                ascending=[True, False, False, True],
            )
            .groupby("wind_kt", sort=True)
            .first()
            .reset_index()
        )

        _best_keys = set(zip(_df_best["wind_kt"], _df_best["n_fcast"]))
        _df_options["best"] = [
            (r["wind_kt"], r["n_fcast"]) in _best_keys
            for _, r in _df_options.iterrows()
        ]

        # Apply best-option triggers to _base for storm table
        for _, _brow in _df_best.iterrows():
            _wkt = int(_brow["wind_kt"])
            _base[f"fcast_trig_{_wkt}"] = _base["sid"].isin(
                _brow["_fcast_sids"]
            )
            _base[f"combined_{_wkt}"] = _base["sid"].isin(
                _brow["_combined_sids"]
            )

        for _wkt in [34, 50, 64]:
            if f"fcast_trig_{_wkt}" not in _base.columns:
                _base[f"fcast_trig_{_wkt}"] = False
            if f"combined_{_wkt}" not in _base.columns:
                _base[f"combined_{_wkt}"] = False

        # Condition flag columns: separate thresholds for fcast and obsv exposure
        _best_thresh = {}
        for _, _r in _df_best.iterrows():
            _wkt = int(_r["wind_kt"])
            _best_thresh[_wkt] = {
                "exp_f": int(_r["exp_thresh_f"]),
                "exp_o": int(_r["exp_thresh_o"])
                if pd.notna(_r["exp_thresh_o"])
                else 0,
                "r_fcast": float(_r["r_fcast_exact"]),
                "r_obs": float(_r["r_obs_exact"])
                if pd.notna(_r["r_obs_exact"])
                else None,
            }
        rain_opt_thresh_and = _best_thresh

        for _wkt, _fcol, _ocol in [
            (34, "fcast_exp_34", "exp_34"),
            (50, "fcast_exp_50", "exp_50"),
            (64, "fcast_exp_64", "exp_64"),
        ]:
            _t = _best_thresh.get(_wkt, {})
            _ef = _t.get("exp_f", float("inf"))
            _eo = _t.get("exp_o", float("inf"))
            _rf = _t.get("r_fcast", float("inf"))
            _ro = _t.get("r_obs")
            _base[f"_fexp_flag_{_wkt}"] = _base[_fcol] >= _ef
            _base[f"_oexp_flag_{_wkt}"] = _base[_ocol] >= _eo
            _base[f"_fcast_rf_{_wkt}"] = _base["max_fcast_rain"].notna() & (
                _base["max_fcast_rain"] >= _rf
            )
            _base[f"_obs_rf_{_wkt}"] = (
                (_base["max_obs_rain"] >= _ro)
                if _ro is not None
                else pd.Series(False, index=_base.index)
            )

        _bool_hide = []
        for _wkt in [34, 50, 64]:
            _base[f"{_wkt} fexp"] = _base[f"_fexp_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} fcast"] = _base[f"_fcast_rf_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} oexp"] = _base[f"_oexp_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} obs"] = _base[f"_obs_rf_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} kt"] = _base[f"fcast_trig_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt}+O"] = _base[f"combined_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _bool_hide += [
                f"_fexp_flag_{_wkt}",
                f"_fcast_rf_{_wkt}",
                f"_oexp_flag_{_wkt}",
                f"_obs_rf_{_wkt}",
                f"fcast_trig_{_wkt}",
                f"combined_{_wkt}",
            ]

        _storm_table = (
            _base[
                [
                    "Storm",
                    "34 fexp",
                    "34 fcast",
                    "34 oexp",
                    "34 obs",
                    "34 kt",
                    "34+O",
                    "50 fexp",
                    "50 fcast",
                    "50 oexp",
                    "50 obs",
                    "50 kt",
                    "50+O",
                    "64 fexp",
                    "64 fcast",
                    "64 oexp",
                    "64 obs",
                    "64 kt",
                    "64+O",
                    *_bool_hide,
                    "Old A|O",
                    "old_combined",
                    "Total Affected",
                    "CERF",
                    "Action",
                    "Mob. trig.",
                    "Obsv. trig.",
                ]
            ]
            .sort_values("Total Affected", ascending=False, na_position="last")
            .reset_index(drop=True)
        )

        _cond_cols = [
            f"{w} {c}"
            for w in [34, 50, 64]
            for c in ["fexp", "fcast", "oexp", "obs"]
        ]
        _trig_cols = [f"{w} kt" for w in [34, 50, 64]]
        _comb_cols = [f"{w}+O" for w in [34, 50, 64]]

        def _style_cond(val):
            if val == "✓":
                return "background-color: #e8f5e9; color: #2e7d32"
            return "color: #ddd"

        def _style_trig(val):
            if val == "✓":
                return "background-color: gold; font-weight: bold"
            return "color: #ccc"

        def _style_combined(val):
            if val == "✓":
                return "background-color: #ffa040; color: white; font-weight: bold"
            return "color: #ccc"

        def _style_check(val):
            if val == "✓":
                return "background-color: #fff0b3; color: #888; font-weight: normal"
            return "color: #ccc"

        def _style_old_combined(val):
            if val == "✓":
                return "background-color: #9c27b0; color: white; font-weight: bold"
            return "color: #ccc"

        def _style_cerf(val):
            if isinstance(val, str) and val.startswith("$"):
                return "background-color: crimson; color: white; font-weight: bold"
            if val == "combined":
                return "background-color: crimson; color: white; font-weight: bold; opacity: 0.6"
            if val == "—":
                return "background-color: #cce5ff; color: #555"
            return "color: #aaa"

        _styled_storms = (
            _storm_table.style.map(_style_cond, subset=_cond_cols)
            .map(_style_trig, subset=_trig_cols)
            .map(_style_combined, subset=_comb_cols)
            .map(_style_old_combined, subset=["Old A|O"])
            .map(_style_check, subset=["Action", "Mob. trig.", "Obsv. trig."])
            .map(_style_cerf, subset=["CERF"])
            .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .hide(axis="columns", subset=_bool_hide + ["old_combined"])
            .hide(axis="index")
        )

        _n_yrs = int(df_exp["season"].max() - df_exp["season"].min() + 1)
        _rp = (_n_yrs + 1) / _n

        _old_sub = _base[_base["old_combined"]]
        _n_old = len(_old_sub)
        _cerf_old = int(_old_sub["has_cerf"].sum())
        _aff_old = int(_old_sub["Total Affected"].fillna(0).sum())
        _rp_old = (_n_yrs + 1) / _n_old if _n_old else float("inf")
        _n_old_fcast = int(_base["action_trig_old"].sum())
        _n_old_obsv_add = int(
            (~_base["action_trig_old"] & _base["obsv_trig"]).sum()
        )

        _summary_rows = [
            {
                "Trigger": "Old (action|obsv)",
                "Wind (fcast)": "≥64 kt speed",
                "Wind (obsv)": "≥50 kt speed",
                "Rain fcast mm": "≥42",
                "Rain obsv mm": "≥70",
                "# Fcast": _n_old_fcast,
                "# Obsv": _n_old_obsv_add,
                "n": _n_old,
                "CERF": _cerf_old,
                "Total Affected": _aff_old,
                "RP yrs": round(_rp_old, 1),
            }
        ]
        for _, _r in _df_best.iterrows():
            _wkt = int(_r["wind_kt"])
            _summary_rows.append(
                {
                    "Trigger": f"New {_wkt} kt ★",
                    "Wind (fcast)": f"≥{int(_r['exp_thresh_f']):,} exp",
                    "Wind (obsv)": f"≥{int(_r['exp_thresh_o']):,} exp"
                    if pd.notna(_r["exp_thresh_o"])
                    else "—",
                    "Rain fcast mm": f"≥{_r['r_fcast']}",
                    "Rain obsv mm": f"≥{_r['r_obs']}"
                    if pd.notna(_r["r_obs"])
                    else "—",
                    "# Fcast": int(_r["n_fcast"]),
                    "# Obsv": int(_r["n_obsv"]),
                    "n": _n,
                    "CERF": int(_r["cerf_count"]),
                    "Total Affected": int(_r["total_affected"]),
                    "RP yrs": round(_rp, 1),
                }
            )
        _df_summary = pd.DataFrame(_summary_rows)

        def _style_summary_row(row):
            _is_old = row["Trigger"] == "Old (action|obsv)"
            _bg = "background-color: #f3e5f5" if _is_old else ""
            return [_bg] * len(row)

        _styled_summary = (
            _df_summary.style.apply(_style_summary_row, axis=1)
            .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if isinstance(x, (int, float))
                    else x
                }
            )
            .set_properties(**{"text-align": "center"})
            .set_properties(
                subset=["Trigger", "Wind (fcast)", "Wind (obsv)"],
                **{"text-align": "left"},
            )
            .set_table_styles(
                [{"selector": "th", "props": [("text-align", "center")]}]
            )
            .hide(axis="index")
        )

        _opt_disp = _df_options[
            [
                "wind_kt",
                "n_fcast",
                "n_obsv",
                "exp_thresh_f",
                "exp_thresh_o",
                "r_fcast",
                "r_obs",
                "cerf_count",
                "total_affected",
                "best",
            ]
        ].copy()
        _opt_disp["r_obs"] = _opt_disp["r_obs"].apply(
            lambda x: str(x) if pd.notna(x) else "—"
        )
        _opt_disp["exp_thresh_o"] = _opt_disp["exp_thresh_o"].apply(
            lambda x: int(x) if pd.notna(x) else "—"
        )
        _opt_disp["best"] = _opt_disp["best"].map({True: "★", False: ""})
        _opt_disp = _opt_disp.rename(
            columns={
                "wind_kt": "Wind kt",
                "n_fcast": "# Fcast",
                "n_obsv": "# Obsv",
                "exp_thresh_f": "Exp (fcast)",
                "exp_thresh_o": "Exp (obsv)",
                "r_fcast": "Rain fcast mm",
                "r_obs": "Rain obsv mm",
                "cerf_count": "CERF #",
                "total_affected": "Total Affected",
                "best": " ",
            }
        )

        def _style_best(val):
            if val == "★":
                return "background-color: gold; font-weight: bold"
            return ""

        _styled_opts = (
            _opt_disp.style.bar(
                subset=["Total Affected"], color="#b39ddb", vmin=0
            )
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .map(_style_best, subset=[" "])
            .hide(axis="index")
        )

        mo.output.replace(
            mo.vstack(
                [
                    mo.md(
                        f"### Rainfall + wind trigger — combined (forecast | obsv), n={_n}  \n"
                        f"★ = best overall per wind level (used in storm table below)."
                    ),
                    mo.md(
                        "**Summary (best option per wind level vs. old trigger):**"
                    ),
                    mo.Html(_styled_summary.to_html()),
                    mo.md(
                        "**All options per wind level × fcast/obsv split:**"
                    ),
                    mo.Html(_styled_opts.to_html()),
                    mo.md("**Storm table (best option per wind level):**"),
                    mo.Html(_styled_storms.to_html()),
                ]
            )
        )
    return df_rain_opt_and, rain_opt_thresh_and


@app.cell
def rain_scatter_and(df_rain_opt_and, mo, pd, plt, rain_opt_thresh_and):
    mo.stop(not rain_opt_thresh_and)

    _WKTS = [34, 50, 64]
    _max_aff = df_rain_opt_and["Total Affected"].fillna(0).max()

    def _bsz(val):
        return max(20, (val / max(_max_aff, 1)) ** 0.5 * 500)

    def _label(row):
        s = row.get("Storm", "")
        return s.split("(")[0].strip() if pd.notna(s) and s else ""

    _fig, _axes = plt.subplots(2, 3, figsize=(18, 10), dpi=120)
    _fig.suptitle(
        "Wind exposure vs. rainfall — trigger conditions (bubble = impact)",
        fontsize=12,
    )

    for _ci, _wkt in enumerate(_WKTS):
        _t = rain_opt_thresh_and[_wkt]
        _fcol = f"fcast_exp_{_wkt}"
        _ocol = f"exp_{_wkt}"
        _e_thresh_f = _t["exp_f"]
        _e_thresh_o = _t.get("exp_o", _t.get("exp_f", 0))
        _r_fcast = _t["r_fcast"]
        _r_obs = _t.get("r_obs")

        # ── Forecast rain (row 0) — x = forecast exposure ──────────────────
        _ax = _axes[0, _ci]
        _df = df_rain_opt_and[
            (df_rain_opt_and[_fcol].fillna(0) > 0)
            & df_rain_opt_and["max_fcast_rain"].notna()
        ].copy()
        _df["_s"] = _df["Total Affected"].fillna(0).apply(_bsz)
        _df["_c"] = "#cccccc"
        _df.loc[_df[f"fcast_trig_{_wkt}"], "_c"] = "gold"
        _df.loc[_df["has_cerf"], "_c"] = "crimson"
        _ax.scatter(
            _df[_fcol],
            _df["max_fcast_rain"],
            s=_df["_s"],
            c=_df["_c"],
            edgecolors="#888",
            linewidths=0.5,
            alpha=0.85,
            zorder=3,
        )
        for _, _row in _df.iterrows():
            _ax.annotate(
                _label(_row),
                xy=(_row[_fcol], _row["max_fcast_rain"]),
                fontsize=6.5,
                ha="center",
                va="bottom",
                color="#333",
            )
        _ax.axvline(
            _e_thresh_f,
            color="goldenrod",
            linestyle="--",
            linewidth=1.2,
            zorder=4,
        )
        _ax.axhline(
            _r_fcast,
            color="goldenrod",
            linestyle="--",
            linewidth=1.2,
            zorder=4,
        )
        _ax.set_xlabel(f"Forecast pop. exposed ({_wkt} kt)")
        _ax.set_ylabel("Forecast rain (mm)" if _ci == 0 else "")
        _ax.set_title(f"{_wkt} kt — forecast rain")
        _ax.grid(True, alpha=0.25, linestyle="--")
        _ax.set_xlim(left=0)
        _ax.set_ylim(bottom=0)

        # ── Observed rain (row 1) — x = observed exposure ──────────────────
        _ax = _axes[1, _ci]
        _df = df_rain_opt_and[df_rain_opt_and[_ocol].fillna(0) > 0].copy()
        _df["_s"] = _df["Total Affected"].fillna(0).apply(_bsz)
        _obs_hit = _df[f"_oexp_flag_{_wkt}"] & _df[f"_obs_rf_{_wkt}"]
        _df["_c"] = "#cccccc"
        _df.loc[_obs_hit, "_c"] = "#ffa040"
        _df.loc[_df["has_cerf"], "_c"] = "crimson"
        _ax.scatter(
            _df[_ocol],
            _df["max_obs_rain"],
            s=_df["_s"],
            c=_df["_c"],
            edgecolors="#888",
            linewidths=0.5,
            alpha=0.85,
            zorder=3,
        )
        for _, _row in _df.iterrows():
            _ax.annotate(
                _label(_row),
                xy=(_row[_ocol], _row["max_obs_rain"]),
                fontsize=6.5,
                ha="center",
                va="bottom",
                color="#333",
            )
        _ax.axvline(
            _e_thresh_o,
            color="#ff8c00",
            linestyle="--",
            linewidth=1.2,
            zorder=4,
        )
        if _r_obs is not None:
            _ax.axhline(
                _r_obs,
                color="#ff8c00",
                linestyle="--",
                linewidth=1.2,
                zorder=4,
            )
        _ax.set_xlabel(f"Observed pop. exposed ({_wkt} kt)")
        _ax.set_ylabel("Observed rain (mm)" if _ci == 0 else "")
        _ax.set_title(f"{_wkt} kt — observed rain")
        _ax.grid(True, alpha=0.25, linestyle="--")
        _ax.set_xlim(left=0)
        _ax.set_ylim(bottom=0)

    plt.tight_layout()
    _fig
    return


@app.cell
def rain_trigger_opt_or(df_rain_opt, mo, pd):
    mo.stop(not len(df_rain_opt))
    _n = 12
    _base = df_rain_opt.copy()
    _opt = _base[
        _base["max_fcast_rain"].notna() | (_base["max_obs_rain"] > 0)
    ].copy()

    # OR sweep: fcast fires if fcast_exp >= thresh_f OR fcast_rain >= r_fcast
    # Obs fires (independently) if obsv_exp >= thresh_o OR obs_rain >= r_obs
    # Separate exposure thresholds per trigger type.
    _results = []
    for _wkt, _fcol, _ocol in [
        (34, "fcast_exp_34", "exp_34"),
        (50, "fcast_exp_50", "exp_50"),
        (64, "fcast_exp_64", "exp_64"),
    ]:
        _exp_f_vals = sorted(_opt[_fcol].unique())
        _exp_o_vals = sorted(_opt[_ocol].unique())
        _fcast_vals = sorted(
            _opt["max_fcast_rain"].dropna().unique(), reverse=True
        )
        for _e_thresh_f in _exp_f_vals:
            for _r_fcast in _fcast_vals:
                _fcast_mask = (_opt[_fcol] >= _e_thresh_f) | (
                    _opt["max_fcast_rain"].notna()
                    & (_opt["max_fcast_rain"] >= _r_fcast)
                )
                _pool_fcast = _opt[_fcast_mask]
                _n_f = len(_pool_fcast)
                if _n_f > _n:
                    continue
                _n_o = _n - _n_f
                _fcast_sids_set = set(_pool_fcast["sid"])
                _not_fcast = _opt[~_fcast_mask]

                if _n_o == 0:
                    _trig_rows = _base[_base["sid"].isin(_fcast_sids_set)]
                    _results.append(
                        {
                            "wind_kt": _wkt,
                            "exp_thresh_f": int(_e_thresh_f),
                            "exp_thresh_o": None,
                            "r_fcast": round(_r_fcast, 1),
                            "r_obs": None,
                            "r_fcast_exact": _r_fcast,
                            "r_obs_exact": None,
                            "n_fcast": _n_f,
                            "n_obsv": 0,
                            "cerf_count": int(_trig_rows["has_cerf"].sum()),
                            "total_affected": int(
                                _trig_rows["Total Affected"].fillna(0).sum()
                            ),
                            "_combined_sids": frozenset(_fcast_sids_set),
                            "_fcast_sids": frozenset(_fcast_sids_set),
                        }
                    )
                else:
                    for _e_thresh_o in _exp_o_vals:
                        _auto_obs = _not_fcast[
                            _not_fcast[_ocol] >= _e_thresh_o
                        ]
                        _n_auto = len(_auto_obs)
                        if _n_auto > _n_o:
                            continue
                        _n_rain_needed = _n_o - _n_auto
                        _rain_pool = _not_fcast[
                            _not_fcast[_ocol] < _e_thresh_o
                        ]
                        if _n_rain_needed == 0:
                            _r_obs = None
                            _obs_new_sids = set(_auto_obs["sid"])
                        else:
                            if len(_rain_pool) < _n_rain_needed:
                                continue
                            _rp_s = _rain_pool.sort_values(
                                "max_obs_rain", ascending=False
                            )
                            _r_obs = float(
                                _rp_s.iloc[_n_rain_needed - 1]["max_obs_rain"]
                            )
                            _obs_rain_sids = set(
                                _rain_pool[
                                    _rain_pool["max_obs_rain"] >= _r_obs
                                ]["sid"]
                            )
                            if len(_obs_rain_sids) != _n_rain_needed:
                                continue
                            _obs_new_sids = (
                                set(_auto_obs["sid"]) | _obs_rain_sids
                            )
                        _combined_sids = _fcast_sids_set | _obs_new_sids
                        if len(_combined_sids) != _n:
                            continue
                        _trig_rows = _base[_base["sid"].isin(_combined_sids)]
                        _results.append(
                            {
                                "wind_kt": _wkt,
                                "exp_thresh_f": int(_e_thresh_f),
                                "exp_thresh_o": int(_e_thresh_o),
                                "r_fcast": round(_r_fcast, 1),
                                "r_obs": round(_r_obs, 1)
                                if _r_obs is not None
                                else None,
                                "r_fcast_exact": _r_fcast,
                                "r_obs_exact": _r_obs,
                                "n_fcast": _n_f,
                                "n_obsv": _n_o,
                                "cerf_count": int(
                                    _trig_rows["has_cerf"].sum()
                                ),
                                "total_affected": int(
                                    _trig_rows["Total Affected"]
                                    .fillna(0)
                                    .sum()
                                ),
                                "_combined_sids": frozenset(_combined_sids),
                                "_fcast_sids": frozenset(_fcast_sids_set),
                            }
                        )

    df_rain_opt_or = _base
    rain_opt_thresh_or = {}

    if not _results:
        mo.output.replace(
            mo.md("⚠ No valid threshold combinations found (OR condition).")
        )
    else:
        _df_opt = pd.DataFrame(_results)

        _df_options = (
            _df_opt.sort_values(
                [
                    "wind_kt",
                    "n_fcast",
                    "cerf_count",
                    "total_affected",
                    "r_fcast",
                ],
                ascending=[True, True, False, False, True],
            )
            .groupby(["wind_kt", "n_fcast"], sort=True)
            .first()
            .reset_index()
        )
        _df_best = (
            _df_opt.sort_values(
                ["wind_kt", "cerf_count", "total_affected", "r_fcast"],
                ascending=[True, False, False, True],
            )
            .groupby("wind_kt", sort=True)
            .first()
            .reset_index()
        )

        _best_keys = set(zip(_df_best["wind_kt"], _df_best["n_fcast"]))
        _df_options["best"] = [
            (r["wind_kt"], r["n_fcast"]) in _best_keys
            for _, r in _df_options.iterrows()
        ]

        for _, _brow in _df_best.iterrows():
            _wkt = int(_brow["wind_kt"])
            _base[f"fcast_trig_{_wkt}"] = _base["sid"].isin(
                _brow["_fcast_sids"]
            )
            _base[f"combined_{_wkt}"] = _base["sid"].isin(
                _brow["_combined_sids"]
            )

        for _wkt in [34, 50, 64]:
            if f"fcast_trig_{_wkt}" not in _base.columns:
                _base[f"fcast_trig_{_wkt}"] = False
            if f"combined_{_wkt}" not in _base.columns:
                _base[f"combined_{_wkt}"] = False

        _best_thresh = {}
        for _, _r in _df_best.iterrows():
            _wkt = int(_r["wind_kt"])
            _best_thresh[_wkt] = {
                "exp_f": int(_r["exp_thresh_f"]),
                "exp_o": int(_r["exp_thresh_o"])
                if pd.notna(_r["exp_thresh_o"])
                else 0,
                "r_fcast": float(_r["r_fcast_exact"]),
                "r_obs": float(_r["r_obs_exact"])
                if pd.notna(_r["r_obs_exact"])
                else None,
            }
        rain_opt_thresh_or = _best_thresh

        _bool_hide = []
        for _wkt, _fcol, _ocol in [
            (34, "fcast_exp_34", "exp_34"),
            (50, "fcast_exp_50", "exp_50"),
            (64, "fcast_exp_64", "exp_64"),
        ]:
            _t = _best_thresh.get(_wkt, {})
            _ef = _t.get("exp_f", float("inf"))
            _eo = _t.get("exp_o", float("inf"))
            _rf = _t.get("r_fcast", float("inf"))
            _ro = _t.get("r_obs")
            _base[f"_fexp_flag_{_wkt}"] = _base[_fcol] >= _ef
            _base[f"_oexp_flag_{_wkt}"] = _base[_ocol] >= _eo
            _base[f"_fcast_rf_{_wkt}"] = _base["max_fcast_rain"].notna() & (
                _base["max_fcast_rain"] >= _rf
            )
            _base[f"_obs_rf_{_wkt}"] = (
                (_base["max_obs_rain"] >= _ro)
                if _ro is not None
                else pd.Series(False, index=_base.index)
            )
            _base[f"{_wkt} fexp"] = _base[f"_fexp_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} fcast"] = _base[f"_fcast_rf_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} oexp"] = _base[f"_oexp_flag_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} obs"] = _base[f"_obs_rf_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt} kt"] = _base[f"fcast_trig_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _base[f"{_wkt}+O"] = _base[f"combined_{_wkt}"].map(
                {True: "✓", False: "—"}
            )
            _bool_hide += [
                f"_fexp_flag_{_wkt}",
                f"_fcast_rf_{_wkt}",
                f"_oexp_flag_{_wkt}",
                f"_obs_rf_{_wkt}",
                f"fcast_trig_{_wkt}",
                f"combined_{_wkt}",
            ]

        df_rain_opt_or = _base

        _storm_table = (
            _base[
                [
                    "Storm",
                    "34 fexp",
                    "34 fcast",
                    "34 oexp",
                    "34 obs",
                    "34 kt",
                    "34+O",
                    "50 fexp",
                    "50 fcast",
                    "50 oexp",
                    "50 obs",
                    "50 kt",
                    "50+O",
                    "64 fexp",
                    "64 fcast",
                    "64 oexp",
                    "64 obs",
                    "64 kt",
                    "64+O",
                    *_bool_hide,
                    "Old A|O",
                    "old_combined",
                    "Total Affected",
                    "CERF",
                    "Action",
                    "Mob. trig.",
                    "Obsv. trig.",
                ]
            ]
            .sort_values("Total Affected", ascending=False, na_position="last")
            .reset_index(drop=True)
        )

        _cond_cols = [
            f"{w} {c}"
            for w in [34, 50, 64]
            for c in ["fexp", "fcast", "oexp", "obs"]
        ]
        _trig_cols = [f"{w} kt" for w in [34, 50, 64]]
        _comb_cols = [f"{w}+O" for w in [34, 50, 64]]

        def _sc(val):
            if val == "✓":
                return "background-color: #e8f5e9; color: #2e7d32"
            return "color: #ddd"

        def _st(val):
            if val == "✓":
                return "background-color: gold; font-weight: bold"
            return "color: #ccc"

        def _sco(val):
            if val == "✓":
                return "background-color: #ffa040; color: white; font-weight: bold"
            return "color: #ccc"

        def _sch(val):
            if val == "✓":
                return "background-color: #fff0b3; color: #888; font-weight: normal"
            return "color: #ccc"

        def _soc(val):
            if val == "✓":
                return "background-color: #9c27b0; color: white; font-weight: bold"
            return "color: #ccc"

        def _scerf(val):
            if isinstance(val, str) and val.startswith("$"):
                return "background-color: crimson; color: white; font-weight: bold"
            if val == "combined":
                return "background-color: crimson; color: white; font-weight: bold; opacity: 0.6"
            if val == "—":
                return "background-color: #cce5ff; color: #555"
            return "color: #aaa"

        _styled_storms = (
            _storm_table.style.map(_sc, subset=_cond_cols)
            .map(_st, subset=_trig_cols)
            .map(_sco, subset=_comb_cols)
            .map(_soc, subset=["Old A|O"])
            .map(_sch, subset=["Action", "Mob. trig.", "Obsv. trig."])
            .map(_scerf, subset=["CERF"])
            .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .hide(axis="columns", subset=_bool_hide + ["old_combined"])
            .hide(axis="index")
        )

        _n_yrs = int(_base["season"].max() - _base["season"].min() + 1)
        _rp = (_n_yrs + 1) / _n

        _old_sub = _base[_base["old_combined"]]
        _n_old = len(_old_sub)
        _cerf_old = int(_old_sub["has_cerf"].sum())
        _aff_old = int(_old_sub["Total Affected"].fillna(0).sum())
        _rp_old = (_n_yrs + 1) / _n_old if _n_old else float("inf")
        _n_old_fcast = int(_base["action_trig_old"].sum())
        _n_old_obsv_add = int(
            (~_base["action_trig_old"] & _base["obsv_trig"]).sum()
        )

        _summary_rows = [
            {
                "Trigger": "Old (action|obsv)",
                "Wind (fcast)": "≥64 kt speed",
                "Wind (obsv)": "≥50 kt speed",
                "Rain fcast mm": "≥42",
                "Rain obsv mm": "≥70",
                "# Fcast": _n_old_fcast,
                "# Obsv": _n_old_obsv_add,
                "n": _n_old,
                "CERF": _cerf_old,
                "Total Affected": _aff_old,
                "RP yrs": round(_rp_old, 1),
            }
        ]
        for _, _r in _df_best.iterrows():
            _wkt = int(_r["wind_kt"])
            _summary_rows.append(
                {
                    "Trigger": f"OR {_wkt} kt ★",
                    "Wind (fcast)": f"≥{int(_r['exp_thresh_f']):,} exp",
                    "Wind (obsv)": f"≥{int(_r['exp_thresh_o']):,} exp"
                    if pd.notna(_r["exp_thresh_o"])
                    else "—",
                    "Rain fcast mm": f"≥{_r['r_fcast']}",
                    "Rain obsv mm": f"≥{_r['r_obs']}"
                    if pd.notna(_r["r_obs"])
                    else "—",
                    "# Fcast": int(_r["n_fcast"]),
                    "# Obsv": int(_r["n_obsv"]),
                    "n": _n,
                    "CERF": int(_r["cerf_count"]),
                    "Total Affected": int(_r["total_affected"]),
                    "RP yrs": round(_rp, 1),
                }
            )
        _df_summary = pd.DataFrame(_summary_rows)

        def _style_summary_row(row):
            _bg = (
                "background-color: #f3e5f5"
                if row["Trigger"] == "Old (action|obsv)"
                else ""
            )
            return [_bg] * len(row)

        _styled_summary = (
            _df_summary.style.apply(_style_summary_row, axis=1)
            .bar(subset=["Total Affected"], color="#b39ddb", vmin=0)
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if isinstance(x, (int, float))
                    else x
                }
            )
            .set_properties(**{"text-align": "center"})
            .set_properties(
                subset=["Trigger", "Wind (fcast)", "Wind (obsv)"],
                **{"text-align": "left"},
            )
            .set_table_styles(
                [{"selector": "th", "props": [("text-align", "center")]}]
            )
            .hide(axis="index")
        )

        _opt_disp = _df_options[
            [
                "wind_kt",
                "n_fcast",
                "n_obsv",
                "exp_thresh_f",
                "exp_thresh_o",
                "r_fcast",
                "r_obs",
                "cerf_count",
                "total_affected",
                "best",
            ]
        ].copy()
        _opt_disp["r_obs"] = _opt_disp["r_obs"].apply(
            lambda x: str(x) if pd.notna(x) else "—"
        )
        _opt_disp["exp_thresh_o"] = _opt_disp["exp_thresh_o"].apply(
            lambda x: int(x) if pd.notna(x) else "—"
        )
        _opt_disp["best"] = _opt_disp["best"].map({True: "★", False: ""})
        _opt_disp = _opt_disp.rename(
            columns={
                "wind_kt": "Wind kt",
                "n_fcast": "# Fcast",
                "n_obsv": "# Obsv",
                "exp_thresh_f": "Exp (fcast)",
                "exp_thresh_o": "Exp (obsv)",
                "r_fcast": "Rain fcast mm",
                "r_obs": "Rain obsv mm",
                "cerf_count": "CERF #",
                "total_affected": "Total Affected",
                "best": " ",
            }
        )

        def _sbest(val):
            return (
                "background-color: gold; font-weight: bold"
                if val == "★"
                else ""
            )

        _styled_opts = (
            _opt_disp.style.bar(
                subset=["Total Affected"], color="#b39ddb", vmin=0
            )
            .format(
                {
                    "Total Affected": lambda x: f"{x:,.0f}"
                    if pd.notna(x)
                    else "—"
                }
            )
            .map(_sbest, subset=[" "])
            .hide(axis="index")
        )

        mo.output.replace(
            mo.vstack(
                [
                    mo.md(
                        f"### Rainfall + wind trigger — OR condition, n={_n}  \n"
                        f"Forecast fires if **exp ≥ thresh OR rain ≥ r_fcast**; "
                        f"obsv fires if **exp ≥ thresh OR rain ≥ r_obs**.  \n"
                        f"★ = best overall per wind level."
                    ),
                    mo.md(
                        "**Summary (best option per wind level vs. old trigger):**"
                    ),
                    mo.Html(_styled_summary.to_html()),
                    mo.md(
                        "**All options per wind level × fcast/obsv split:**"
                    ),
                    mo.Html(_styled_opts.to_html()),
                    mo.md("**Storm table (best option per wind level):**"),
                    mo.Html(_styled_storms.to_html()),
                ]
            )
        )
    return df_rain_opt_or, rain_opt_thresh_or


@app.cell
def rain_scatter_or(df_rain_opt_or, mo, pd, plt, rain_opt_thresh_or):
    mo.stop(not rain_opt_thresh_or)

    _WKTS = [34, 50, 64]
    _max_aff = df_rain_opt_or["Total Affected"].fillna(0).max()

    def _bsz(val):
        return max(20, (val / max(_max_aff, 1)) ** 0.5 * 500)

    def _label(row):
        s = row.get("Storm", "")
        return s.split("(")[0].strip() if pd.notna(s) and s else ""

    _fig, _axes = plt.subplots(2, 3, figsize=(18, 10), dpi=120)
    _fig.suptitle(
        "Wind exposure vs. rainfall — OR trigger conditions (bubble = impact)",
        fontsize=12,
    )

    for _ci, _wkt in enumerate(_WKTS):
        _t = rain_opt_thresh_or[_wkt]
        _fcol = f"fcast_exp_{_wkt}"
        _ocol = f"exp_{_wkt}"
        _e_thresh_f = _t["exp_f"]
        _e_thresh_o = _t.get("exp_o", _t.get("exp_f", 0))
        _r_fcast = _t["r_fcast"]
        _r_obs = _t.get("r_obs")

        # ── Forecast rain (row 0) — x = forecast exposure ──────────────────
        _ax = _axes[0, _ci]
        _df = df_rain_opt_or[
            (df_rain_opt_or[_fcol].fillna(0) > 0)
            & df_rain_opt_or["max_fcast_rain"].notna()
        ].copy()
        _df["_s"] = _df["Total Affected"].fillna(0).apply(_bsz)
        _df["_c"] = "#cccccc"
        _df.loc[_df[f"fcast_trig_{_wkt}"], "_c"] = "gold"
        _df.loc[_df["has_cerf"], "_c"] = "crimson"
        _ax.scatter(
            _df[_fcol],
            _df["max_fcast_rain"],
            s=_df["_s"],
            c=_df["_c"],
            edgecolors="#888",
            linewidths=0.5,
            alpha=0.85,
            zorder=3,
        )
        for _, _row in _df.iterrows():
            _ax.annotate(
                _label(_row),
                xy=(_row[_fcol], _row["max_fcast_rain"]),
                fontsize=6.5,
                ha="center",
                va="bottom",
                color="#333",
            )
        _ax.axvline(
            _e_thresh_f,
            color="goldenrod",
            linestyle="--",
            linewidth=1.2,
            zorder=4,
        )
        _ax.axhline(
            _r_fcast,
            color="goldenrod",
            linestyle="--",
            linewidth=1.2,
            zorder=4,
        )
        _ax.set_xlabel(f"Forecast pop. exposed ({_wkt} kt)")
        _ax.set_ylabel("Forecast rain (mm)" if _ci == 0 else "")
        _ax.set_title(f"{_wkt} kt — forecast rain (OR)")
        _ax.grid(True, alpha=0.25, linestyle="--")
        _ax.set_xlim(left=0)
        _ax.set_ylim(bottom=0)

        # ── Observed rain (row 1) — x = observed exposure ──────────────────
        _ax = _axes[1, _ci]
        _df = df_rain_opt_or[df_rain_opt_or[_ocol].fillna(0) > 0].copy()
        _df["_s"] = _df["Total Affected"].fillna(0).apply(_bsz)
        _obs_hit = _df[f"_oexp_flag_{_wkt}"] | _df[f"_obs_rf_{_wkt}"]
        _df["_c"] = "#cccccc"
        _df.loc[_obs_hit, "_c"] = "#ffa040"
        _df.loc[_df["has_cerf"], "_c"] = "crimson"
        _ax.scatter(
            _df[_ocol],
            _df["max_obs_rain"],
            s=_df["_s"],
            c=_df["_c"],
            edgecolors="#888",
            linewidths=0.5,
            alpha=0.85,
            zorder=3,
        )
        for _, _row in _df.iterrows():
            _ax.annotate(
                _label(_row),
                xy=(_row[_ocol], _row["max_obs_rain"]),
                fontsize=6.5,
                ha="center",
                va="bottom",
                color="#333",
            )
        _ax.axvline(
            _e_thresh_o,
            color="#ff8c00",
            linestyle="--",
            linewidth=1.2,
            zorder=4,
        )
        if _r_obs is not None:
            _ax.axhline(
                _r_obs,
                color="#ff8c00",
                linestyle="--",
                linewidth=1.2,
                zorder=4,
            )
        _ax.set_xlabel(f"Observed pop. exposed ({_wkt} kt)")
        _ax.set_ylabel("Observed rain (mm)" if _ci == 0 else "")
        _ax.set_title(f"{_wkt} kt — observed rain (OR)")
        _ax.grid(True, alpha=0.25, linestyle="--")
        _ax.set_xlim(left=0)
        _ax.set_ylim(bottom=0)

    plt.tight_layout()
    _fig
    return


if __name__ == "__main__":
    app.run()
