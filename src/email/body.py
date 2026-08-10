"""HTML email bodies (French), ds-storms-alerts inline-style conventions.

No jinja: f-string composition with inline styles (Gmail/Outlook safe).
The Listmonk campaign template supplies the outer chrome (header bar,
footer, unsubscribe); these functions build only the inner body.
"""

import pandas as pd

from src.constants import (
    LT_CUTOFF_HRS,
    OVERALL_RP_YEARS,
    STAGE_NAMES_FR,
    TRIGGERS,
)
from src.email.plots import fmt_pop, fr_datetime

_H2 = "font-size:1.35em;margin:24px 0 8px;font-weight:600"
_H3 = "font-size:1.1em;margin:16px 0 6px;font-weight:600;color:#3f4748"
_TD = "padding:8px 12px;vertical-align:top;border-bottom:1px solid #ebeff0"
_TH = (
    "padding:9px 12px;text-align:left;font-weight:600;font-size:0.76em;"
    "color:#5e6a6b;text-transform:uppercase;letter-spacing:0.05em;"
    "border-bottom:2px solid #d8e0e1;white-space:nowrap"
)
_HR = "<hr style='border:none;border-top:1px solid #e2e8e8;margin:24px 0'>"

_PILL_ON = (
    "display:inline-block;padding:2px 12px;border-radius:999px;"
    "background:#9d372b;color:#ffffff;font-weight:600;font-size:0.85em"
)
_PILL_OFF = (
    "display:inline-block;padding:2px 12px;border-radius:999px;"
    "background:#e2e8e8;color:#5e6a6b;font-weight:600;font-size:0.85em"
)
_PILL_NA = (
    "display:inline-block;padding:2px 12px;border-radius:999px;"
    "background:#fbf4ea;color:#8a6116;font-weight:600;font-size:0.85em"
)

DISCLAIMER_HTML = (
    "<div style='font-size:11px;padding:8px 12px;background:#fafbfb;"
    "border:1px solid #ebeff0;border-radius:6px;color:#5e6a6b'>"
    "Cet e-mail est purement consultatif et ne sert pas d'avis officiel "
    "pour le cadre d'action anticipatoire. Les avis officiels "
    "d'activation sont envoyés par un autre e-mail. Cet e-mail ne "
    "constitue pas non plus une prévision officielle. Le "
    "<a href='https://www.meteo-haiti.gouv.ht/cyclone.php'>Communiqué "
    "d'activité cyclonique</a> publié par l'Unité HydroMétéorologique "
    "d'Haïti est la source officielle pour les informations locales. "
    "Dans le cadre du Tropical Cyclone Programme de l'Organisation "
    "météorologique mondiale, RSMC Miami, géré par le National "
    "Hurricane Center (NHC) de NOAA, est chargé de surveiller les "
    "cyclones tropicaux dans la région ; "
    "<a href='https://www.nhc.noaa.gov/cyclones/'>leurs prévisions des "
    "cyclones</a> font autorité.</div>"
)

FOOTER_HTML = (
    "<p style='color:#5e6a6b;font-size:0.9em'>Le code utilisé pour "
    "produire cette alerte est disponible sur "
    "<a href='https://github.com/OCHA-DAP/ds-aa-hti-hurricanes'>"
    "GitHub</a>.</p>"
    "<p>Meilleures salutations,</p>"
    "<p>Centre de données humanitaires OCHA</p>"
)


def past_cutoff_callout(time_to_closest: pd.Timedelta) -> str:
    hrs = max(0, int(time_to_closest.total_seconds() // 3600))
    return (
        "<div style='background:#fbf4ea;border-left:4px solid #d48f2a;"
        "padding:10px 14px;border-radius:0 6px 6px 0;margin:14px 0'>"
        "<b>Délai de déclenchement dépassé.</b> Le passage au plus près "
        f"d'Haïti est prévu dans environ {hrs} h, soit moins de "
        f"{LT_CUTOFF_HRS} h : le cadre ne peut plus être déclenché sur "
        "la base des prévisions pour cette tempête. Cet e-mail est "
        "purement informatif. Le déclencheur observationnel "
        "(Réponse précoce) reste actif.</div>"
    )


def _cond_row(label: str, value: str, thresh: str, met: bool) -> str:
    check = (
        "<span style='color:#9d372b;font-weight:700'>✓ atteint</span>"
        if met
        else "<span style='color:#7e8e8f'>— non atteint</span>"
    )
    return (
        f"<tr><td style='{_TD}'>{label}</td>"
        f"<td style='{_TD};text-align:right'>{value}</td>"
        f"<td style='{_TD};text-align:right'>{thresh}</td>"
        f"<td style='{_TD}'>{check}</td></tr>"
    )


def stage_panel(
    stage: str,
    conditions: list[tuple[str, str, str, bool]],
    active: bool,
    not_applicable: bool = False,
    note: str | None = None,
) -> str:
    """One trigger stage: name, status pill, conditions table.

    conditions: (label, current value, threshold, met) rows.
    """
    name = STAGE_NAMES_FR[stage]
    rp = TRIGGERS[stage]["rp_years"]
    lt = TRIGGERS[stage]["lt_max_hrs"]
    lt_str = f" · prévisions ≤ {lt} h" if lt else " · observations"
    if not_applicable:
        pill = f"<span style='{_PILL_NA}'>DÉLAI DÉPASSÉ</span>"
    elif active:
        pill = f"<span style='{_PILL_ON}'>ACTIVÉ</span>"
    else:
        pill = f"<span style='{_PILL_OFF}'>NON ACTIVÉ</span>"
    rows = "".join(_cond_row(*c) for c in conditions)
    note_html = (
        f"<p style='color:#7e8e8f;font-size:0.85em;margin:4px 0'>{note}</p>"
        if note
        else ""
    )
    return (
        f"<h3 style='{_H3}'>{name} {pill}</h3>"
        f"<p style='color:#7e8e8f;font-size:0.85em;margin:2px 0 8px'>"
        f"Période de retour ≈ {str(rp).replace('.', ',')} ans{lt_str}</p>"
        "<table style='border-collapse:collapse;width:100%;"
        "font-size:0.95em'>"
        f"<thead><tr><th style='{_TH}'>Condition</th>"
        f"<th style='{_TH};text-align:right'>Valeur actuelle</th>"
        f"<th style='{_TH};text-align:right'>Seuil</th>"
        f"<th style='{_TH}'>Statut</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>{note_html}"
    )


def _fcast_stage_conditions(row: pd.Series, stage: str) -> list:
    lt = TRIGGERS[stage]["lt_max_hrs"]
    rain = row[f"rain_{lt}h"]
    exp = row[f"exp_total_{lt}h"]
    rain_str = "—" if pd.isnull(rain) else f"{rain:.0f} mm"
    return [
        (
            f"Précipitations prévues sur 2 jours (≤ {lt} h)",
            rain_str,
            f"≥ {TRIGGERS[stage]['rain_mm']} mm",
            bool(row[f"rain_trigger_{stage}"]),
        ),
        (
            f"Population prévue exposée aux vents ≥ 64 kt (≤ {lt} h)",
            fmt_pop(exp) if not pd.isnull(exp) else "—",
            "> 0 personnes",
            bool(row[f"exp_trigger_{stage}"]),
        ),
    ]


def _obsv_stage_conditions(row: pd.Series) -> list:
    rain = row["obsv_rain"]
    exp = row["obsv_exp"]
    return [
        (
            "Précipitations observées sur 2 jours (IMERG)",
            "—" if pd.isnull(rain) else f"{rain:.0f} mm",
            f"≥ {TRIGGERS['obsv']['rain_mm']} mm",
            bool(row["rain_trigger_obsv"]),
        ),
        (
            "Population observée exposée aux vents ≥ 64 kt",
            fmt_pop(exp) if not pd.isnull(exp) else "—",
            "> 0 personnes",
            bool(row["exp_trigger_obsv"]),
        ),
    ]


def build_fcast_info_body(
    row: pd.Series, wsp_img: str, map_img: str | None
) -> str:
    name = row["name"]
    issue_str = fr_datetime(row["issue_time"])
    past_cutoff = bool(row["past_cutoff"])

    intro = (
        "<p>Chers collègues,</p>"
        f"<p>Les prévisions pour <b>{name}</b> émises le {issue_str} "
        "(heure locale Haïti) par le National Hurricane Center (NHC) de "
        "NOAA viennent d'être analysées, avec les prévisions de "
        "précipitations CHIRPS-GEFS (UC Santa Barbara).</p>"
    )
    cutoff_html = (
        past_cutoff_callout(row["time_to_closest"]) if past_cutoff else ""
    )
    panels = (
        f"<h2 style='{_H2}'>Statut du déclencheur</h2>"
        + stage_panel(
            "mobilisation",
            _fcast_stage_conditions(row, "mobilisation"),
            bool(row["mobilisation_trigger"]),
            not_applicable=past_cutoff,
        )
        + stage_panel(
            "action",
            _fcast_stage_conditions(row, "action"),
            bool(row["action_trigger"]),
            not_applicable=past_cutoff,
            note=(
                "La condition « alerte rouge DGPC confirmée par un "
                "Hurricane Warning du NHC » n'est pas encore suivie par "
                "ce système."
            ),
        )
        + (
            f"<p style='color:#5e6a6b;font-size:0.9em'>Période de retour "
            f"globale du cadre ≈ "
            f"{str(OVERALL_RP_YEARS).replace('.', ',')} ans.</p>"
        )
    )
    plots = (
        f"<h2 style='{_H2}'>Prévisions probabilistes</h2>"
        "<p style='color:#5e6a6b;font-size:0.9em'>Probabilité que la "
        "population d'Haïti exposée à chaque niveau de vent atteigne "
        "une valeur donnée, selon les Wind Speed Probabilities du NHC "
        "(l'exposition prévue par la trajectoire déterministe utilisée "
        "pour le déclencheur figure dans le tableau ci-dessus).</p>" + wsp_img
    )
    map_html = (f"<h2 style='{_H2}'>Carte</h2>" + map_img) if map_img else ""
    return (
        DISCLAIMER_HTML
        + intro
        + cutoff_html
        + _HR
        + panels
        + _HR
        + plots
        + map_html
        + _HR
        + FOOTER_HTML
    )


def build_obsv_info_body(row: pd.Series, map_img: str | None) -> str:
    name = row["name"]
    issue_str = fr_datetime(row["issue_time"])
    intro = (
        "<p>Chers collègues,</p>"
        f"<p>Les observations pour <b>{name}</b> au {issue_str} (heure "
        "locale Haïti) viennent d'être analysées : précipitations "
        "observées IMERG (NASA) et trajectoire observée du NHC.</p>"
    )
    panels = f"<h2 style='{_H2}'>Statut du déclencheur</h2>" + stage_panel(
        "obsv",
        _obsv_stage_conditions(row),
        bool(row["obsv_trigger"]),
    )
    map_html = (f"<h2 style='{_H2}'>Carte</h2>" + map_img) if map_img else ""
    return (
        DISCLAIMER_HTML + intro + _HR + panels + map_html + _HR + FOOTER_HTML
    )


def build_trigger_body(
    row: pd.Series,
    stage: str,
    wsp_img: str | None,
    map_img: str | None,
) -> str:
    """Stage activation notice (mobilisation / action / obsv)."""
    name = row["name"]
    issue_str = fr_datetime(row["issue_time"])
    stage_fr = STAGE_NAMES_FR[stage]
    if stage == "obsv":
        conditions = _obsv_stage_conditions(row)
        basis = "des observations"
    else:
        conditions = _fcast_stage_conditions(row, stage)
        basis = "des prévisions"
    intro = (
        "<p>Chers collègues,</p>"
        f"<p>Le seuil de déclenchement <b>{stage_fr}</b> du cadre "
        "d'action anticipatoire pour les ouragans en Haïti vient d'être "
        f"<b>ATTEINT</b> pour <b>{name}</b>, sur la base {basis} du "
        f"{issue_str} (heure locale Haïti).</p>"
    )
    panel = stage_panel(stage, conditions, active=True)
    plots = (wsp_img or "") + (map_img or "")
    return DISCLAIMER_HTML + intro + _HR + panel + _HR + plots + FOOTER_HTML
