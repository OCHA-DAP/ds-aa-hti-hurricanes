"""Render the department-level DGPC page (``docs/dgpc-departements.html``).

A second, narrower page than ``dgpc-alertes.html``: now that DGPC has
said alerts are issued by department and the wind criterion is the wind in
the department, this page asks one question storm by storm - would the
framework have activated, and how many departments would DGPC have placed
in orange on the forecasts available at the time?

The earlier page is kept as an archive of the wider assessment (three
spatial scales, observed as well as forecast wind, the red level).
"""

from datetime import date
from html import escape

import numpy as np
import pandas as pd

from src.constants import D_THRESH
from src.dgpc import constants as dc
from src.dgpc.dept_forecast import READINGS
from src.dgpc.page import CSS, MONTHS_FR, _count_cell, _mark, fr_num, plural

# South-west to north-east, the order a reader scans a map of Haiti.
DEPT_ORDER = [
    "Grande'Anse",
    "Sud",
    "Nippes",
    "Sud-Est",
    "Ouest",
    "Centre",
    "Artibonite",
    "Nord-Ouest",
    "Nord",
    "Nord-Est",
]

SHORT = {
    "wind": "Vent",
    "rain_mean": "Pluie<br>moy. dép.",
    "rain_pix": "Pluie<br>point",
    "any_mean": "Vent <i>ou</i><br>pluie moy.",
    "any_pix": "Vent <i>ou</i><br>pluie point",
}

EXTRA_CSS = """
td.val{text-align:right;font-variant-numeric:tabular-nums;color:#5e6a6b}
td.val.hit{background:#fdefe7;color:#a4551f;font-weight:700}
td.val.na{color:#c2caccc;text-align:center}
tr.dim td{color:#9aa5a6}
tr.dim td.nm{color:#5e6a6b;font-weight:500}
.legend{font-size:.86rem;color:var(--muted)}
table.matrix th{font-size:.74rem}
table.matrix td{padding:.35rem .5rem;font-size:.84rem}
"""


def _val_cell(v, thr, na=False, decimals=0):
    if na or v is None or (np.isscalar(v) and pd.isna(v)):
        return "<td class='val na'>n/d</td>"
    cls = "val hit" if v >= thr else "val"
    return f"<td class='{cls}'>{fr_num(v, decimals)}</td>"


def _summary_rows(counts, n_years):
    rows = []
    for k, label in READINGS.items():
        col = f"n_dept_{k}"
        known = counts[col].notna()
        hit = counts[known & (counts[col] > 0)]
        ny = hit["season"].nunique()
        rp = (n_years + 1) / ny if ny else np.inf
        med = hit[col].median() if len(hit) else np.nan
        lead = hit[f"lead_h_{k}"].median() if len(hit) else np.nan
        rows.append(
            f"<tr><td class='nm'>{escape(label)}</td>"
            f"<td class='num'>{len(hit)} / {int(known.sum())}</td>"
            f"<td class='num'>{ny}</td>"
            f"<td class='num'>"
            f"{fr_num(rp, 1) if np.isfinite(rp) else 'jamais'}</td>"
            f"<td class='num'>{fr_num(med) if np.isfinite(med) else '—'}</td>"
            f"<td class='num'>{fr_num(lead) if np.isfinite(lead) else '—'}"
            "</td></tr>"
        )
    return "\n".join(rows)


def _main_table(tbl):
    """Framework activation beside the DGPC orange count, per storm."""
    rows = []
    for _, r in tbl.iterrows():
        na = pd.isna(r.get("atcf_id"))
        in_deck = bool(r.get("in_deck", False))
        cls = "" if in_deck else " class='dim'"
        trig = r.get("triggered_hit")
        trig_cell = (
            _mark(trig)
            if in_deck
            else "<td style='text-align:center;color:#c2cacc' "
            "title='Absente du registre : pas d’activation'>—</td>"
        )
        cells = "".join(
            _count_cell(
                np.nan if na else r.get(f"n_dept_{k}"), r.get(f"depts_{k}", "")
            )
            for k in READINGS
        )
        pop = r.get("pop_affected_n")
        cerf = r.get("cerf")
        cerf = "" if cerf is None or pd.isna(cerf) else str(cerf)
        rows.append(
            f"<tr{cls}><td class='nm'>{escape(str(r['label']))}</td>"
            f"<td class='num'>{fr_num(r.get('min_dist_km'))}</td>"
            f"{trig_cell}{cells}"
            f"<td class='num'>{fr_num(pop)}</td>"
            f"<td>{escape(cerf) or '—'}</td></tr>"
        )

    tot = {"cadre": int(tbl["triggered_hit"].fillna(False).astype(bool).sum())}
    for k in READINGS:
        tot[k] = int((tbl[f"n_dept_{k}"].fillna(0) > 0).sum())
    heads = "".join(
        f"<th style='text-align:center'>{SHORT[k]}</th>" for k in READINGS
    )
    tot_cells = "".join(
        f"<td style='text-align:center'>{tot[k]}</td>" for k in READINGS
    )
    return f"""
<div class="tablewrap"><table class="wraphead">
<thead>
<tr>
  <th rowspan="2">Tempête</th>
  <th rowspan="2">Distance<br>min. (km)</th>
  <th rowspan="2">Déclenchement<br>du cadre</th>
  <th colspan="{len(READINGS)}" style="text-align:center">
    Départements en alerte orange DGPC (sur 10), selon la lecture</th>
  <th rowspan="2">Pop.<br>affectée</th>
  <th rowspan="2">CERF</th>
</tr>
<tr>{heads}</tr>
</thead>
<tbody>
{chr(10).join(rows)}
</tbody>
<tfoot><tr style="background:#f7fafb;font-weight:700">
  <td>Total ({len(tbl)} lignes)</td><td></td>
  <td style="text-align:center">{tot["cadre"]}</td>
  {tot_cells}
  <td></td><td></td>
</tr></tfoot>
</table></div>
"""


def _matrix(verdicts, storms_order, col, thr, decimals=0):
    """Storms x departments, one value per cell, shaded where met."""
    piv = verdicts.pivot(index="atcf_id", columns="dept", values=col)
    avail = verdicts.pivot(
        index="atcf_id", columns="dept", values="n_rain_fcasts"
    )
    rows = []
    for _, s in storms_order.iterrows():
        aid = s["atcf_id"]
        cells = []
        for d in DEPT_ORDER:
            v = piv.at[aid, d] if aid in piv.index else np.nan
            na = (
                col.startswith("rain")
                and aid in avail.index
                and avail.at[aid, d] == 0
            )
            cells.append(_val_cell(v, thr, na=na, decimals=decimals))
        rows.append(
            f"<tr><td class='nm'>{escape(str(s['label']))}</td>"
            + "".join(cells)
            + "</tr>"
        )
    heads = "".join(f"<th>{escape(d)}</th>" for d in DEPT_ORDER)
    return f"""
<div class="tablewrap"><table class="matrix">
<thead><tr><th>Tempête</th>{heads}</tr></thead>
<tbody>
{chr(10).join(rows)}
</tbody></table></div>
"""


def _lead_table(counts):
    d = counts[
        (counts[[f"n_dept_{k}" for k in READINGS]].fillna(0) > 0).any(axis=1)
    ]
    rows = []
    for _, r in d.iterrows():
        cells = []
        for k in ("wind", "rain_mean", "rain_pix"):
            v = r.get(f"lead_h_{k}")
            n = r.get(f"n_dept_{k}")
            if pd.isna(n):
                cells.append("<td class='val na'>n/d</td>")
            elif n == 0 or pd.isna(v):
                cells.append("<td class='val'>—</td>")
            else:
                style = " style='color:#a02a2a'" if v < 0 else ""
                cells.append(f"<td class='val'{style}>{fr_num(v)}</td>")
        rows.append(
            f"<tr><td class='nm'>{escape(str(r['label']))}</td>"
            + "".join(cells)
            + "</tr>"
        )
    return f"""
<div class="tablewrap"><table class="wraphead">
<thead><tr><th>Tempête</th><th>Vent</th><th>Pluie — moyenne dép.</th>
<th>Pluie — point</th></tr></thead>
<tbody>
{chr(10).join(rows)}
</tbody></table></div>
"""


def _freq_table(freq):
    order = {d: i for i, d in enumerate(DEPT_ORDER)}
    f = freq.assign(_o=freq["dept"].map(order)).sort_values("_o")
    rows = []
    for _, r in f.iterrows():
        cells = []
        for k in READINGS:
            n = int(r[f"n_storms_{k}"])
            rp = r[f"rp_{k}"]
            rp_txt = f"{fr_num(rp, 1)} ans" if np.isfinite(rp) else "jamais"
            cells.append(
                f"<td class='num'>{n}<br>"
                f"<span style='color:#5e6a6b;font-size:.85em'>{rp_txt}</span>"
                "</td>"
            )
        rows.append(
            f"<tr><td class='nm'>{escape(str(r['dept']))}</td>"
            + "".join(cells)
            + "</tr>"
        )
    heads = "".join(f"<th>{SHORT[k]}</th>" for k in READINGS)
    return f"""
<div class="tablewrap"><table class="wraphead">
<thead><tr><th>Département</th>{heads}</tr></thead>
<tbody>
{chr(10).join(rows)}
</tbody></table></div>
"""


def render(tbl, verdicts, counts, freq, chart="", notes=None):
    """Assemble the page.

    ``tbl`` is the per-storm table (storm set joined to the deck's
    activation record); ``verdicts`` the storm x department rows;
    ``counts`` and ``freq`` the per-storm and per-department summaries.
    """
    today = date.today()
    stamp = f"{MONTHS_FR[today.month]} {today.year}"
    n_years = dc.SEASON_END - dc.SEASON_START + 1
    n_storms = int(tbl["atcf_id"].notna().sum())
    notes = notes or {}

    n_trig = int(tbl["triggered_hit"].fillna(False).astype(bool).sum())
    n_wind = int((counts["n_dept_wind"].fillna(0) > 0).sum())
    n_mean = int((counts["n_dept_rain_mean"].fillna(0) > 0).sum())
    n_pix = int((counts["n_dept_rain_pix"].fillna(0) > 0).sum())
    n_rain_eval = int(counts["rain_available"].sum())

    both_mean = tbl["triggered_hit"].fillna(False).astype(bool) & (
        tbl["n_dept_any_mean"].fillna(0) > 0
    )
    trig_only = (
        tbl["triggered_hit"].fillna(False).astype(bool)
        & (tbl["n_dept_any_mean"].fillna(0) == 0)
        & tbl["n_dept_any_mean"].notna()
    )
    orange_only = ~tbl["triggered_hit"].fillna(False).astype(bool) & (
        tbl["n_dept_any_mean"].fillna(0) > 0
    )

    def lbl(mask):
        return ", ".join(escape(str(x)) for x in tbl.loc[mask, "label"])

    no_rain = counts.loc[~counts["rain_available"], "label"].tolist()
    gap_note = ""
    if no_rain:
        gap_note = (
            f"<b>{', '.join(escape(str(s)) for s in no_rain)}</b> : aucune "
            "prévision CHIRPS-GEFS archivée (l’archive s’interrompt de janvier "
            "à septembre 2020) ; les cases de pluie sont « n/d », pas 0."
        )

    storms_order = tbl[tbl["atcf_id"].notna()][["atcf_id", "label"]]
    thr_mm = dc.ORANGE_RAIN["threshold_mm"]

    figure = (
        f"<figure>{chart}<figcaption>Nombre de départements en alerte "
        "orange par tempête, selon la lecture, pour les tempêtes qui en "
        "placent au moins un.</figcaption></figure>"
        if chart
        else ""
    )

    return f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Alerte orange de la DGPC par département — simulation historique</title>
<style>{CSS}{EXTRA_CSS}</style>
</head>
<body>
<div class="wrap">

<header>
  <p class="crumb"><a href="./">←
  Action anticipatoire · Ouragans · Haïti</a></p>
  <div class="kicker">Analyse · {escape(stamp)}</div>
  <h1>Alerte orange de la DGPC par département : simulation historique
  sur prévisions</h1>
  <p class="sub">
    Pour chacune des {n_storms} tempêtes passées à moins de {D_THRESH} km
    d’Haïti entre {dc.SEASON_START} et {dc.SEASON_END} : le cadre
    aurait-il été activé, et combien de départements la DGPC aurait-elle
    placés en alerte orange sur la base des prévisions disponibles ?
  </p>
</header>

<p class="lead">
  En septembre 2026, la DGPC a précisé que ses alertes sont émises
  <b>par département</b> et que le critère de vent désigne le vent
  <b>dans le département</b>. Cette page refait la simulation à cette
  échelle, uniquement sur des <b>prévisions</b> — celles que le cadre
  utilise déjà : CHIRPS-GEFS pour la pluie, les avis du NHC pour le vent.
  L’évaluation plus large (trois échelles spatiales, vent observé, niveau
  rouge) reste consultable dans
  <a href="dgpc-alertes.html">la première analyse, conservée en archive</a>.
</p>

<div class="stats">
  <div class="stat"><div class="big">{n_trig}</div>
    <div class="lab">{plural(n_trig, "tempête")} pour
    {plural(n_trig, "laquelle", "lesquelles")} le <b>cadre</b> se serait
    activé (registre des diapositives)</div></div>
  <div class="stat"><div class="big">{n_wind}</div>
    <div class="lab">{plural(n_wind, "tempête")} avec au moins un
    département en orange <b>vent</b> (≥ 100 km/h prévu)</div></div>
  <div class="stat"><div class="big">{n_mean}</div>
    <div class="lab">{plural(n_mean, "tempête")} avec au moins un
    département en orange <b>pluie</b>, moyenne du département
    (≥ 100 mm/24 h prévu ; {n_rain_eval} évaluées)</div></div>
  <div class="stat"><div class="big">{n_pix}</div>
    <div class="lab">{plural(n_pix, "tempête")} avec au moins un
    département en orange <b>pluie</b>, point le plus arrosé
    ({n_rain_eval} évaluées)</div></div>
</div>

<h2>1. Les lectures comparées</h2>

<p>
  Le critère orange de la DGPC est « 100 mm/24 h de pluie <i>ou</i>
  100–120 km/h de vent ». Appliqué par département et sur prévisions, il
  laisse une question ouverte : la pluie s’entend-elle en moyenne sur le
  département ou au point le plus arrosé ? Les deux lectures sont
  présentées, seules et combinées au vent.
</p>

<div class="tablewrap"><table class="prose">
<thead><tr><th>Lecture</th><th>Un département est en orange si…</th>
<th>Source</th></tr></thead>
<tbody>
<tr><td class="nm">Vent</td>
    <td>le champ de vent prévu (vent soutenu sur terre, réduction ×0,85)
        atteint 100 km/h en un point du département, pour au moins un avis
        du NHC émis pendant l’approche</td>
    <td>Avis du NHC, champ paramétrique</td></tr>
<tr><td class="nm">Pluie — moyenne du département</td>
    <td>la moyenne du département d’une prévision journalière atteint
        100 mm, pour au moins une émission pendant l’approche</td>
    <td>CHIRPS-GEFS, 0,05°</td></tr>
<tr><td class="nm">Pluie — point le plus arrosé</td>
    <td>un pixel (5,5 km) du département atteint 100 mm dans une prévision
        journalière</td>
    <td>CHIRPS-GEFS, 0,05°</td></tr>
<tr><td class="nm">Vent <i>ou</i> pluie</td>
    <td>l’une des deux conditions ci-dessus — c’est la définition
        complète du niveau orange</td>
    <td>—</td></tr>
</tbody></table></div>

<h2>2. Le cadre et l’alerte orange, tempête par tempête</h2>

<p>
  Toutes les tempêtes du jeu, dans l’ordre chronologique. La colonne
  « déclenchement du cadre » reprend le registre des activations des
  diapositives ; les tempêtes absentes de ce registre (en gris) n’ont ni
  déclenché, ni causé d’impact recensé, ni reçu d’allocation CERF. Les
  colonnes DGPC donnent le <b>nombre de départements</b> (sur 10) qui
  auraient été en alerte orange ; survoler une case pour lire lesquels.
  {gap_note}
  {notes.get("deck", "")}
</p>

{_main_table(tbl)}

<p class="legend">
  ✓ = cadre activé ; — = non activé ; n/d = hors du jeu analysé ou
  prévision indisponible. Population affectée : EM-DAT, reprise des
  diapositives. CERF : « pre- » = antérieur à la création du CERF (2006) ;
  « combined » = allocation combinée Fay/Gustav/Hanna/Ike (2008).
</p>

<div class="callout">
  <h3>Où les deux systèmes se rejoignent, et où ils divergent</h3>
  <p>
    Sous la lecture « vent <i>ou</i> pluie en moyenne départementale »,
    <b>{int(both_mean.sum())}</b> {plural(int(both_mean.sum()), "tempête")}
    {plural(int(both_mean.sum()), "aurait", "auraient")} à la fois activé
    le cadre et placé au moins un département en orange ({lbl(both_mean)}).
  </p>
  <p>
    Le cadre se serait activé <b>sans</b> qu’aucun département ne soit en
    orange pour {int(trig_only.sum())} {plural(int(trig_only.sum()), "tempête")}
    ({lbl(trig_only) or "aucune"}) ; à l’inverse, au moins un département
    aurait été en orange <b>sans</b> activation du cadre pour
    {int(orange_only.sum())} {plural(int(orange_only.sum()), "tempête")}
    ({lbl(orange_only) or "aucune"}).
  </p>
</div>

{figure}

<h2>3. Combien de tempêtes placent au moins un département en orange</h2>

<div class="tablewrap"><table class="wraphead">
<thead><tr>
  <th>Lecture</th><th>Tempêtes<br>(sur évaluées)</th><th>Saisons</th>
  <th>Période de<br>retour (ans)</th>
  <th>Départements<br>par tempête (médiane)</th>
  <th>Préavis médian<br>(h avant passage)</th>
</tr></thead>
<tbody>
{_summary_rows(counts, n_years)}
</tbody></table></div>

<p>
  Période de retour : (n + 1) / k sur {n_years} saisons, k étant le nombre
  de saisons avec au moins une tempête concernée. Le cadre lui-même vise
  une période de retour globale de {fr_num(dc.FRAMEWORK_RP_YEARS, 1)} ans.
  Le préavis est le délai entre la première prévision atteignant le
  critère et le passage au plus près.
</p>

<h2>4. Le détail par département</h2>

<p>
  Les trois matrices ci-dessous donnent, pour chaque tempête et chaque
  département, la valeur maximale prévue pendant l’approche. Les cases
  orange atteignent le seuil. Départements du sud-ouest au nord-est.
</p>

<h3>Vent prévu maximal dans le département (km/h)</h3>
{_matrix(verdicts, storms_order, "wind_max_kmh", dc.ORANGE_WIND_KMH[0])}

<h3>Pluie prévue — moyenne du département (mm / jour)</h3>
{_matrix(verdicts, storms_order, "rain_mean_max_mm", thr_mm)}

<h3>Pluie prévue — point le plus arrosé du département (mm / jour)</h3>
{_matrix(verdicts, storms_order, "rain_pix_max_mm", thr_mm)}

<h2>5. Préavis</h2>

<p>
  Pour les tempêtes ayant placé au moins un département en orange : heures
  entre la première prévision atteignant le critère (dans n’importe quel
  département) et le passage au plus près. Une valeur négative (en rouge)
  signifie que le seuil n’a été prévu qu’après le passage — l’alerte
  aurait été tardive.
</p>

{_lead_table(counts)}

<h2>6. Fréquence par département</h2>

<p>
  Nombre de tempêtes ayant placé chaque département en orange sur
  {dc.SEASON_START}–{dc.SEASON_END}, et période de retour correspondante.
</p>

{_freq_table(freq)}

<h2>7. Méthode et limites</h2>

<ul>
  <li><b>Tempêtes</b> : centre analysé passé à moins de {D_THRESH} km
      d’Haïti, {dc.SEASON_START}–{dc.SEASON_END} — le jeu de la première
      analyse. Le registre du cadre retient aussi des tempêtes plus
      lointaines ayant causé des impacts (Ivan 2004), marquées n/d.</li>
  <li><b>Prévisions de pluie</b> : CHIRPS-GEFS émis de {dc.FCAST_LEAD_DAYS}
      jours avant le premier passage à moins de {D_THRESH} km jusqu’au
      dernier ; jours de validité attribués de la veille du premier
      passage au lendemain du dernier. Produit <b>journalier</b> : la
      valeur d’un jour calendaire tient lieu du cumul sur 24 h, ce qui
      sous-estime légèrement un cumul glissant. Moyennes départementales
      sur un ré-échantillonnage à {dc.GEFS_UPSAMPLE_RES}°.</li>
  <li><b>Prévisions de vent</b> : champ de vent paramétrique reconstruit à
      partir des rayons de vent de chaque avis du NHC, vent soutenu réduit
      sur terre (×0,85), maximum dans chaque département. Méthode et
      validation dans l’analyse archivée.</li>
  <li><b>Une prévision suffit</b> : un département est compté en orange
      dès qu’une émission pendant l’approche atteint le critère, quelle
      que soit l’échéance — c’est la lecture la plus favorable à l’alerte,
      et c’est pourquoi le vent prévu place bien plus de tempêtes en
      orange que le vent observé (voir l’archive).</li>
  <li><b>Registre du cadre</b> : repris des diapositives, qui font foi ;
      les tempêtes qui n’y figurent pas n’ont pas activé le cadre.</li>
</ul>

<footer>
  OCHA · Centre de données humanitaires — analyse préparée pour la révision
  2026 du cadre d’action anticipatoire pour les tempêtes et ouragans en
  Haïti. Code et méthode :
  <a href="https://github.com/OCHA-DAP/ds-aa-hti-hurricanes"
  >ds-aa-hti-hurricanes</a>.
  Données : NHC (avis de prévision), CHIRPS-GEFS (UCSB CHC), WorldPop 2026,
  EM-DAT. Mise à jour {escape(stamp)}.
</footer>

</div>
</body>
</html>
"""
