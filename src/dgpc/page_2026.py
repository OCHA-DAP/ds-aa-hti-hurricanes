"""Render the 2026 framework page (``docs/2026/index.html``).

One page for the endorsed 2026 version of the framework: the trigger as
it stands, the deck, the advisory-by-advisory replay on a map with the
storm-by-storm record, and every analysis that went into the choice -
the rain and exposure thresholds, the DGPC orange alert by department,
and the check that the DGPC red + NHC Hurricane Warning pathway cannot
fire before the heure limite.

The framework document links to this page, so its return periods use the
framework's convention: seasons in the record divided by seasons with at
least one activation (24 / 10 = 2.4 years overall).
"""

from datetime import date
from html import escape

import numpy as np
import pandas as pd

from src.constants import (
    D_THRESH,
    EXPOSURE_WIND_KT,
    LT_CUTOFF_HRS,
    OVERALL_RP_YEARS,
    TRIGGERS,
)
from src.dgpc import constants as dc
from src.dgpc.dept_forecast import READINGS
from src.dgpc.map_page import MAP_CSS, MAP_HTML, activation_table, map_js
from src.dgpc.page import CSS, MONTHS_FR, _count_cell, _mark, fr_num, plural
from src.dgpc.pathways import PATHWAYS, flags

N_YEARS = dc.SEASON_END - dc.SEASON_START + 1
ORANGE_N = dc.ORANGE_N_DEPTS
RAIN_FCAST = TRIGGERS["action"]["rain_mm"]
RAIN_OBSV = TRIGGERS["obsv"]["rain_mm"]

# The framework document's return periods per stage. Action, Réponse
# précoce and the overall figure are recomputed from the replay below and
# must agree; Mobilisation (120 h cap) is not in the replay, which reads
# forecast rain at the Action cap, so the document's figure is quoted.
# (src.constants.TRIGGERS, which the monitoring emails print, holds the
# same figures; the monitoring has neither DGPC pathway.)
DOC_RP = {"mobilisation": 3.0, "action": 2.7, "obsv": 3.4}

# The Haiti Hurricane Warnings the first calibration (exploration/
# wsp_trigger.py, May-June 2026) had hand-coded; the audited list is
# data/nhc_hurricane_warnings_hti.csv.
HAND_CODED_HW = {"AL072008", "AL212010", "AL142016", "AL112017", "AL052021"}

# The storms the orange pathway adds at ORANGE_N, which the page's prose
# names and explains; the build stops if the replay adds others.
ORANGE_ADDS = {"AL092008", "AL092011", "AL032013"}  # Ike, Irene, Chantal

# Every pathway but red + Hurricane Warning, in the order the page lists
# them; the stage rows that group them.
OTHER_PATHWAYS = ("fcast_exp", "fcast_rain", "orange", "obsv_exp", "obsv_rain")
STAGE_ROWS = ("action", "obsv", "all")

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
td.val.na{color:#9aa5a6;text-align:center}
td.val.pre{background:#fdf6ec;color:#8a5a12;font-weight:700;
box-shadow:inset 0 0 0 1px #e8cfa8}
tr.dim td{color:#9aa5a6}
tr.dim td.nm{color:#5e6a6b;font-weight:500}
.legend{font-size:.86rem;color:var(--muted)}
table.matrix th{font-size:.74rem}
table.matrix td{padding:.35rem .5rem;font-size:.84rem}
.home-link{display:inline-block;margin:0 0 1.4rem;padding:6px 12px;
font:500 13px/1 'Roboto',system-ui,sans-serif;color:#1e795f;background:#e9f5f1;
border:1px solid #d4eae4;border-radius:4px;text-decoration:none}
.home-link:hover{background:#d4eae4}
header{padding-top:2.2rem}
nav.toc{background:var(--card);border:1px solid var(--line);border-radius:10px;
padding:1rem 1.3rem;margin:0 0 1rem;max-width:44em;font-size:.94rem}
nav.toc b{display:block;font-size:.76rem;text-transform:uppercase;
letter-spacing:.08em;color:var(--muted);margin-bottom:.4rem}
nav.toc ol{margin:0 0 0 1.2rem}
nav.toc li{margin-bottom:.15rem}
nav.toc a{color:var(--blue);text-decoration:none}
nav.toc a:hover{text-decoration:underline}
h2[id],h3[id]{scroll-margin-top:1rem}
table.def td{white-space:normal;vertical-align:middle}
table.def td.num{white-space:nowrap}
table.def td.stage{font-weight:700;white-space:nowrap}
table.def tr.overall td{background:#f7fafb;font-weight:700}
table.def .ou{display:inline-block;background:var(--blue);color:#fff;
font-weight:800;border-radius:999px;padding:0 .55rem;font-size:.72rem;
margin:.25rem 0}
table.def .et{display:inline-block;background:var(--red);color:#fff;
font-weight:800;border-radius:999px;padding:0 .55rem;font-size:.72rem;
margin:0 .3rem}
table.def .cond{display:block}
table.def .official{display:block;margin-top:.15rem;padding:.35rem .55rem;
background:#fdf6ec;border-radius:6px}
.deck{position:relative;width:100%;aspect-ratio:16/10;min-height:520px;
border:1px solid var(--line);border-radius:10px;overflow:hidden;
background:var(--card);margin:1.2rem 0 .8rem}
.deck iframe{position:absolute;inset:0;width:100%;height:100%;border:0}
.btns{display:flex;flex-wrap:wrap;gap:.6rem;margin:.4rem 0 1.2rem}
.btns a{display:inline-block;padding:.5rem 1rem;border-radius:8px;
text-decoration:none;font-weight:600;font-size:.92rem}
.btns a.primary{background:var(--blue);color:#fff}
.btns a.secondary{background:var(--card);color:var(--ink);
border:1px solid var(--line)}
details.more{margin:1.2rem 0;border:1px solid var(--line);border-radius:10px;
background:var(--card);padding:.2rem 1.1rem}
details.more>summary{cursor:pointer;font-weight:700;padding:.7rem 0}
details.more[open]>summary{border-bottom:1px solid var(--line);
margin-bottom:.6rem}
.answer{font-size:1.12rem;font-weight:600;max-width:40em}
"""


def _rp(n_seasons):
    """Return period in years, framework convention."""
    return N_YEARS / n_seasons if n_seasons else np.inf


def _h(hours):
    """Hours, with a decimal only where there is one (13,5 h; 54 h)."""
    hours = float(hours)
    return fr_num(hours, 0 if hours.is_integer() else 1)


def _and(names):
    """'A, B et C'."""
    if len(names) < 2:
        return "".join(names)
    return ", ".join(names[:-1]) + " et " + names[-1]


def _de(name):
    """'de Matthew', 'd’Isaac': French elision before a vowel."""
    return f"d’{name}" if name[:1].upper() in "AEIOUYÉ" else f"de {name}"


def _rp_txt(n_seasons):
    rp = _rp(n_seasons)
    return fr_num(rp, 1) if np.isfinite(rp) else "jamais"


def _val_cell(v, thr, na=False, decimals=0, missing="n/d"):
    if na or v is None or (np.isscalar(v) and pd.isna(v)):
        return f"<td class='val na'>{missing}</td>"
    cls = "val hit" if v >= thr else "val"
    return f"<td class='{cls}'>{fr_num(v, decimals)}</td>"


def _summary_rows(counts):
    rows = []
    for k, label in READINGS.items():
        col = f"n_dept_{k}"
        known = counts[col].notna()
        hit = counts[known & (counts[col] > 0)]
        ny = hit["season"].nunique()
        med = hit[col].median() if len(hit) else np.nan
        lead = hit[f"lead_h_{k}"].median() if len(hit) else np.nan
        rows.append(
            f"<tr><td class='nm'>{escape(label)}</td>"
            f"<td class='num'>{len(hit)} / {int(known.sum())}</td>"
            f"<td class='num'>{ny}</td>"
            f"<td class='num'>{_rp_txt(ny)}</td>"
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
        for d in dc.DEPT_ORDER:
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
    heads = "".join(f"<th>{escape(d)}</th>" for d in dc.DEPT_ORDER)
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
    order = {d: i for i, d in enumerate(dc.DEPT_ORDER)}
    f = freq.assign(_o=freq["dept"].map(order)).sort_values("_o")
    rows = []
    for _, r in f.iterrows():
        cells = []
        for k in READINGS:
            n = int(r[f"n_storms_{k}"])
            ny = int(r[f"n_years_{k}"])
            rp_txt = f"{_rp_txt(ny)} ans" if ny else "jamais"
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


def stage_flags(pdf, warnings):
    """Per-storm flags for each pathway and stage of the 2026 trigger.

    The red + Hurricane Warning pathway is credited with every storm whose
    warning came on an advisory before the heure limite - an upper bound,
    since DGPC's red alert, the other half of the condition, has no
    archive.
    """
    fl = flags(pdf, RAIN_FCAST, RAIN_OBSV, ORANGE_N)
    pre = set(warnings.loc[warnings["pre_cutoff"], "atcf_id"])
    fl["red_hw"] = pdf["atcf_id"].isin(pre)
    fl["action"] = fl[["fcast_exp", "fcast_rain", "orange", "red_hw"]].any(
        axis=1
    )
    fl["obsv"] = fl[["obsv_exp", "obsv_rain"]].fillna(False).any(axis=1)
    fl["all"] = fl["action"] | fl["obsv"]
    return fl


def check_record(pdf, fl):
    """Fail the build if the replay no longer matches what the page says.

    The page states that the red + Hurricane Warning pathway adds no
    activation, and quotes the framework document's return periods; both
    are recomputed here rather than trusted.
    """
    # Red + Hurricane Warning is an Action condition: it adds something
    # whenever it fires for a storm the other Action pathways miss, even
    # one that Réponse précoce would catch later.
    action_others = fl[["fcast_exp", "fcast_rain", "orange"]].any(axis=1)
    adds = fl["red_hw"] & ~action_others
    if adds.any():
        names = ", ".join(pdf.loc[adds, "label"].astype(str))
        raise RuntimeError(f"red + Hurricane Warning adds an Action: {names}")
    got = {
        "action": round(_rp(pdf.loc[fl["action"], "season"].nunique()), 1),
        "obsv": round(_rp(pdf.loc[fl["obsv"], "season"].nunique()), 1),
        "all": round(_rp(pdf.loc[fl["all"], "season"].nunique()), 1),
    }
    want = {
        "action": DOC_RP["action"],
        "obsv": DOC_RP["obsv"],
        "all": OVERALL_RP_YEARS,
    }
    if got != want:
        raise RuntimeError(f"return periods {got} != framework {want}")


def _definition_section(n_act, n_seasons):
    ou = '<span class="ou">OU</span>'
    rain_f = f"≥ {RAIN_FCAST} mm de précipitations prévues"
    exp_f = (
        "&gt; 0 personnes prévues d’être exposées aux vents de "
        f"{EXPOSURE_WIND_KT} nœuds"
    )
    return f"""
<p>
  Trois déclencheurs — <b>Mobilisation</b>, <b>Action</b> et <b>Réponse
  précoce</b>. Pour chaque déclencheur, une seule condition suffit. Les
  conditions de précipitations et d’exposition sont évaluées
  automatiquement à chaque avis du National Hurricane Center (quatre fois
  par jour) ; les deux conditions d’alertes officielles reposent sur les
  alertes déclarées par la DGPC et, pour la seconde, sur le Hurricane
  Warning du NHC.
</p>

<div class="tablewrap"><table class="def">
<thead><tr><th>Déclencheur</th><th>Période<br>de retour</th>
<th>Délai maximum<br>des prévisions</th>
<th>Conditions de déclenchement</th></tr></thead>
<tbody>
<tr>
  <td class="stage">Prévisions :<br>Mobilisation</td>
  <td class="num">environ {fr_num(DOC_RP["mobilisation"], 1)} ans</td>
  <td class="num">{TRIGGERS["mobilisation"]["lt_max_hrs"]} heures</td>
  <td><span class="cond">{rain_f}</span>{ou}
      <span class="cond">{exp_f}</span></td>
</tr>
<tr>
  <td class="stage">Prévisions :<br>Action</td>
  <td class="num">{fr_num(DOC_RP["action"], 1)} ans</td>
  <td class="num">{TRIGGERS["action"]["lt_max_hrs"]} heures</td>
  <td><span class="cond">{rain_f}</span>{ou}
      <span class="cond">{exp_f}</span>{ou}
      <span class="cond official">La DGPC déclare l’alerte orange dans au
      moins {ORANGE_N} départements (sur 10)</span>{ou}
      <span class="cond official">La DGPC lance une alerte rouge
      <span class="et">ET</span> l’alerte est confirmée par le NOAA NHC
      avec un « Hurricane Warning »</span></td>
</tr>
<tr>
  <td class="stage">Observations :<br>Réponse précoce</td>
  <td class="num">{fr_num(DOC_RP["obsv"], 1)} ans</td>
  <td class="num">—</td>
  <td><span class="cond">≥ {RAIN_OBSV} mm de précipitations
      observées</span>{ou}
      <span class="cond">&gt; 0 personnes observées d’être exposées aux
      vents de {EXPOSURE_WIND_KT} nœuds</span></td>
</tr>
<tr class="overall">
  <td>Période de retour globale</td>
  <td class="num">{fr_num(OVERALL_RP_YEARS, 1)} ans</td><td></td>
  <td>{n_act} tempêtes en {n_seasons} saisons sur
      {dc.SEASON_START}–{dc.SEASON_END}</td>
</tr>
</tbody></table></div>

<div class="note">
  <h3>Heure limite et règles d’activation</h3>
  <ul>
    <li><b>Heure limite :</b> aucun déclencheur « prévisions » ne peut
        s’activer à moins de {LT_CUTOFF_HRS} heures du passage prévu au
        plus près d’Haïti — les deux conditions d’alertes officielles
        comprises. Après le passage, seule la Réponse précoce
        (observations) reste possible.</li>
    <li>Chaque stade s’active au plus une fois par tempête ; une activation
        est acquise pour toute la durée de la tempête. La Réponse précoce
        est omise si le stade Action a déjà activé.</li>
    <li><b>Précipitations :</b> cumul maximal sur 2 jours, moyenne
        nationale — CHIRPS-GEFS pour les prévisions, IMERG (NASA) pour les
        observations. <b>Exposition :</b> population dans la zone de vents
        de 64 nœuds prévue par le NHC (ou effectivement traversée).</li>
    <li><b>Périodes de retour :</b> par saison sur {dc.SEASON_START}–{dc.SEASON_END}
        ({N_YEARS} saisons), divisées par le nombre de saisons avec au
        moins une activation.</li>
  </ul>
</div>
"""


def _deck_section():
    return """
<p>
  La présentation du mécanisme : chaque voie d’activation expliquée pas à
  pas avec des exemples, l’heure limite, et le registre des activations
  historiques. Flèches ← → ou boutons pour naviguer.
</p>
<div class="deck">
  <iframe src="slides.html" title="Diapositives : le mécanisme de
  déclenchement 2026" loading="lazy" allowfullscreen></iframe>
</div>
<div class="btns">
  <a class="primary" href="slides.html">▶ Ouvrir en plein écran</a>
  <a class="secondary" href="assets/declencheur-aa-haiti-ouragans.pdf"
     download="declencheur-aa-haiti-ouragans-2026.pdf">⬇ Télécharger le PDF</a>
</div>
"""


def _map_section(pdf, meta, warnings):
    no_gefs_list = pdf.loc[~pdf["orange_rain_known"].astype(bool), "label"]
    no_gefs = _and(no_gefs_list.astype(str).tolist())
    n_no_gefs = len(no_gefs_list)
    return f"""
<p>
  Le mécanisme rejoué sur les {len(pdf)} tempêtes passées à moins de
  {D_THRESH} km d’Haïti entre {dc.SEASON_START} et {dc.SEASON_END}.
  Choisir une tempête, puis faire défiler les avis du NHC : pour chaque
  avis, la trajectoire et les rayons de vent prévus, la bande de vents déjà
  observée, les départements que la prévision de pluie et le champ de
  rafales auraient placés en alerte orange, le Hurricane Warning une fois
  émis, et l’état des voies de déclenchement à cet instant.
</p>

<div class="callout">
  <h3>Les alertes de la DGPC affichées sont une simulation, pas un registre</h3>
  <p>
    La DGPC ne dispose pas d’un historique de ses alertes. Les départements
    « en orange » sur cette carte et dans le tableau sont une
    <b>estimation</b> : les seuils de la DGPC (100 mm en 24 h, rafales de
    100 km/h), appliqués par nos soins aux prévisions de l’époque. Pour la
    voie rouge, seul le Hurricane Warning du NHC peut être rejoué.
  </p>
</div>

{MAP_HTML}

<p class="legend" style="max-width:none">
  <b>Lecture.</b> Orange plein : département en orange par les rafales
  <i>et</i> la pluie ; hachures : rafales seules ; points : pluie seule ;
  orange pâle : déjà en orange sur un avis précédent émis avant l’heure
  limite. Les rayons de vent sont les polygones du NHC à 34, 50 et
  64 nœuds, construits comme pour le déclencheur d’exposition ; la bande
  observée est la zone déjà balayée par des vents de 64 nœuds à
  l’émission, que l’exposition prévue exclut. Les avis émis moins de
  {LT_CUTOFF_HRS} h avant le passage au plus près sont marqués « après
  l’heure limite » : ils ne peuvent plus déclencher le cadre et sont
  affichés en transparence. Flèches ← → pour changer d’avis.
</p>

<h3 id="registre">Tempête par tempête : ce qui aurait déclenché, et pourquoi</h3>

<p style="max-width:none">
  Chaque tempête du jeu, par population affectée décroissante, avec la
  valeur de chaque indicateur sur les avis émis avant l’heure limite.
  Cases orange : seuil atteint. La colonne <b>activation</b> est le
  verdict d’ensemble du mécanisme 2026. Cliquer sur une tempête pour la
  charger dans la carte.
</p>

{activation_table(pdf, meta, warnings)}

<p class="legend" style="max-width:none">
  Entre crochets, en petit : la même valeur en ignorant l’heure limite —
  ce que les prévisions ont fini par dire, même trop tard pour agir ; en
  orange lorsque ce maximum tardif atteint le seuil. Exposition en
  personnes ; précipitations en mm sur deux jours glissants (moyenne
  nationale, CHIRPS-GEFS en prévision, IMERG en observation) ; alerte
  orange en nombre de départements (sur 10). <b>Hurricane Warning</b> :
  heures entre l’avis du NHC qui l’a émis pour Haïti et le passage au plus
  près qu’il prévoyait ; encadré lorsque l’avis précède l’heure limite.
  La condition exige aussi l’alerte rouge de la DGPC, dont il n’existe pas
  d’archive (voir la <a href="#rouge">section 6</a>). « — » en prévision :
  aucun avis avant l’heure limite ne prévoyait de passage à moins de
  {D_THRESH} km. {no_gefs} {plural(n_no_gefs, "n’a", "n’ont")} pas de
  prévision CHIRPS-GEFS archivée : le compte de départements ne tient
  qu’au vent. Population
  affectée : EM-DAT ; Melissa 2025 : chiffres préliminaires. CERF :
  « pre- » = antérieur à la création du CERF (2006) ; « combined » =
  allocation combinée Fay/Gustav/Hanna/Ike (2008).
</p>
"""


def _thresholds_section(pdf, fl):
    rows = []
    labels = dict(PATHWAYS)
    labels["orange"] = f"Alerte orange DGPC (≥ {ORANGE_N} départements)"
    labels[
        "red_hw"
    ] = "Alerte rouge DGPC + Hurricane Warning (avant l’heure limite)"
    order = ["fcast_exp", "fcast_rain", "orange", "red_hw"]
    for k in order + ["action", "obsv_exp", "obsv_rain", "obsv", "all"]:
        hit = fl[k].fillna(False).astype(bool)
        ny = int(pdf.loc[hit, "season"].nunique())
        names = ", ".join(pdf.loc[hit, "label"].astype(str))
        lab = {
            "action": "<b>Stade Action</b> (l’une des quatre voies)",
            "obsv": "<b>Réponse précoce</b> (l’une des deux voies)",
            "all": "<b>Mécanisme complet</b>",
        }.get(k, escape(labels.get(k, k)))
        style = " style='background:#f7fafb'" if k in STAGE_ROWS else ""
        rows.append(
            f"<tr{style}><td style='white-space:normal'>{lab}</td>"
            f"<td class='num'>{int(hit.sum())}</td><td class='num'>{ny}</td>"
            f"<td class='num'>{_rp_txt(ny)}</td>"
            f"<td style='white-space:normal'>{escape(names) or '—'}</td></tr>"
        )
    no_orange = (
        fl[["fcast_exp", "fcast_rain", "obsv_exp", "obsv_rain"]]
        .fillna(False)
        .any(axis=1)
    )
    n_no, y_no = int(no_orange.sum()), int(
        pdf.loc[no_orange, "season"].nunique()
    )
    n_seasons_all = int(pdf.loc[fl["all"], "season"].nunique())
    return f"""
<p>
  Les seuils ont été fixés sur une simulation historique : chaque avis du
  NHC émis pour les {len(pdf)} tempêtes passées à moins de {D_THRESH} km
  d’Haïti entre {dc.SEASON_START} et {dc.SEASON_END}, rejoué avec le code
  du système de suivi — prévisions de précipitations CHIRPS-GEFS associées
  à chaque avis, précipitations observées IMERG, et population exposée aux
  vents de 64 nœuds dans les rayons de vent du NHC. Seuls les avis émis
  avant l’heure limite comptent pour les voies de prévision.
</p>
<ul>
  <li><b>Exposition :</b> le seuil retenu est la plus petite valeur non
      nulle observée dans l’historique, soit en pratique « plus de
      0 personne » exposée aux vents de 64 nœuds.</li>
  <li><b>Précipitations :</b> les seuils de {RAIN_FCAST} mm (prévus) et
      {RAIN_OBSV} mm (observés) ont été choisis en mai–juin 2026 par un
      balayage de toutes les combinaisons de seuils, en visant une
      douzaine de tempêtes activées sur la période et, parmi les
      combinaisons possibles, celle qui retient le plus de tempêtes ayant
      reçu une allocation CERF, puis le plus de population affectée.</li>
  <li><b>Alerte orange :</b> ajoutée en septembre 2026 au stade Action,
      seuils de précipitations et d’exposition inchangés ; le nombre de
      départements est discuté à la <a href="#orange">section 5</a>.</li>
  <li><b>Alerte rouge + Hurricane Warning :</b> vérifiée en octobre 2026
      contre l’heure limite ; elle n’ajoute aucune tempête
      (<a href="#rouge">section 6</a>).</li>
</ul>

<h3>Période de retour de chaque voie</h3>

<div class="tablewrap"><table class="wraphead">
<thead><tr><th>Voie</th><th>Tempêtes</th><th>Saisons</th>
<th>Période de<br>retour (ans)</th><th>Tempêtes concernées</th></tr></thead>
<tbody>
{chr(10).join(rows)}
</tbody></table></div>

<p class="legend">
  Période de retour : {N_YEARS} saisons divisées par le nombre de saisons
  avec au moins une tempête concernée ; la formule de Weibull,
  ({N_YEARS} + 1) / k, donnerait des valeurs légèrement plus longues
  ({fr_num((N_YEARS + 1) / n_seasons_all, 1)} ans pour le mécanisme
  complet). La ligne « alerte rouge + Hurricane Warning » compte les
  tempêtes dont le Hurricane Warning précède l’heure limite : un majorant,
  l’alerte rouge de la DGPC n’étant pas archivée. Sans la voie orange, le mécanisme
  retiendrait {n_no} tempêtes en {y_no} saisons ({_rp_txt(y_no)} ans). La
  Mobilisation (échéance de 120 h) n’est pas reprise de ce rejeu, qui lit
  les précipitations prévues à l’échéance Action ; sa période de retour
  est celle du document du cadre.
</p>
"""


def _orange_section(tbl, verdicts, counts, freq, pdf, fl, chart, notes):
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

    # The ladder: every department count, the other thresholds untouched.
    base = (
        fl[["fcast_exp", "fcast_rain", "obsv_exp", "obsv_rain"]]
        .fillna(False)
        .any(axis=1)
    )
    ladder, n_by = [], {}
    for n in range(1, 11):
        act = flags(pdf, RAIN_FCAST, RAIN_OBSV, n)["any"]
        n_by[n] = int(act.sum())
        yrs = int(pdf.loc[act, "season"].nunique())
        added = ", ".join(pdf.loc[act & ~base, "label"].astype(str))
        chosen = n == ORANGE_N
        style = " style='background:#fdefe7;font-weight:700'" if chosen else ""
        pill = " <span class='pill orange'>retenu</span>" if chosen else ""
        ladder.append(
            f"<tr{style}><td class='num'>{n}{pill}</td>"
            f"<td class='num'>{int(act.sum())}</td>"
            f"<td class='num'>{yrs}</td><td class='num'>{_rp_txt(yrs)}</td>"
            f"<td style='white-space:normal'>{escape(added) or '—'}</td></tr>"
        )
    act_n = fl["all"]
    n_all = int(act_n.sum())
    y_all = int(pdf.loc[act_n, "season"].nunique())
    added_n = ", ".join(pdf.loc[fl["orange"] & ~base, "label"].astype(str))
    n_base = int(base.sum())
    y_base = int(pdf.loc[base, "season"].nunique())
    edge = (pdf["n_orange"] == ORANGE_N - 1) & ~base
    n_edge = int(edge.sum())
    edge_names = _and(pdf.loc[edge, "label"].astype(str).tolist())
    got_adds = set(pdf.loc[fl["orange"] & ~base, "atcf_id"])
    if got_adds != ORANGE_ADDS:
        raise RuntimeError(
            f"orange now adds {sorted(got_adds)}; update the prose naming "
            f"Ike, Irene and Chantal, and ORANGE_ADDS"
        )

    return f"""
<p class="lead">
  En septembre 2026, la DGPC a précisé que ses alertes sont émises
  <b>par département</b> et que le critère de vent désigne le vent
  <b>dans le département</b>. La simulation est donc faite à cette
  échelle, uniquement sur les <b>prévisions</b> que le cadre utilise déjà :
  CHIRPS-GEFS pour la pluie, les avis du NHC pour le vent. Une première
  évaluation, plus large, est conservée
  <a href="premiere-evaluation.html">en annexe</a>.
</p>

<div class="stats">
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
  <div class="stat"><div class="big">{int(fl["orange"].sum())}</div>
    <div class="lab">tempêtes avec au moins {ORANGE_N} départements en
    orange, vent ou pluie au point — le seuil du cadre</div></div>
</div>

<h3>Les lectures comparées</h3>

<p>
  Le critère orange de la DGPC est « 100 mm/24 h de pluie <i>ou</i>
  100–120 km/h de vent » (Plan SAPMAH Cyclone 2025, p. 32). Appliqué par
  département et sur prévisions, il laisse une question ouverte : la pluie
  s’entend-elle en moyenne sur le département ou au point le plus arrosé ?
  Les deux lectures sont présentées, seules et combinées au vent. Le cadre
  retient les rafales et le point le plus arrosé, la lecture que la DGPC
  applique.
</p>

<div class="tablewrap"><table class="prose">
<thead><tr><th>Lecture</th><th>Un département est en orange si…</th>
<th>Source</th></tr></thead>
<tbody>
<tr><td class="nm">Vent</td>
    <td>les <b>rafales</b> prévues (vent soutenu sur terre ×0,85, facteur de
        rafale ×1,25) atteignent 100 km/h en un point du département, pour
        au moins un avis du NHC émis avant l’heure limite</td>
    <td>Avis du NHC, champ paramétrique</td></tr>
<tr><td class="nm">Pluie — moyenne du département</td>
    <td>la moyenne du département d’une prévision journalière atteint
        100 mm, pour la prévision CHIRPS-GEFS associée à un avis émis avant
        l’heure limite</td>
    <td>CHIRPS-GEFS, 0,05°</td></tr>
<tr><td class="nm">Pluie — point le plus arrosé</td>
    <td>un pixel (5,5 km) du département atteint 100 mm dans cette même
        prévision journalière — la lecture que la DGPC applique</td>
    <td>CHIRPS-GEFS, 0,05°</td></tr>
<tr><td class="nm">Vent <i>ou</i> pluie</td>
    <td>l’une des deux conditions ci-dessus — c’est la définition
        complète du niveau orange</td>
    <td>—</td></tr>
</tbody></table></div>

<h3>Combien de départements ?</h3>

<p>
  Sans la voie orange, les seuils de précipitations et d’exposition
  retiennent {n_base} tempêtes en {y_base} saisons. Le tableau donne,
  pour chaque nombre minimal de départements en orange, le registre
  obtenu en gardant ces seuils tels quels.
</p>

<div class="tablewrap"><table class="wraphead">
<thead><tr><th>Orange dans<br>≥ N départements</th><th>Tempêtes</th>
<th>Saisons</th><th>Période de<br>retour (ans)</th>
<th>Tempêtes ajoutées par la voie orange</th></tr></thead>
<tbody>
{chr(10).join(ladder)}
</tbody></table></div>

<p>
  {edge_names} {plural(n_edge, "compte", "comptent chacune")}
  {ORANGE_N - 1} départements, si bien que le registre passe de
  {n_by[ORANGE_N - 1]} tempêtes à {ORANGE_N - 1} départements à
  {n_by[ORANGE_N]} à {ORANGE_N}. <b>Le cadre retient {ORANGE_N} départements</b> :
  {n_all} tempêtes en {y_all} saisons, période de retour
  {_rp_txt(y_all)} ans, la voie orange ajoutant {added_n}. Ike 2008 est
  la tempête à fort impact que les autres voies manquent ; Irene et
  Chantal sont le coût de la lecture en rafales.
</p>

<details class="more">
<summary>Le cadre et l’alerte orange, tempête par tempête (toutes les
lectures)</summary>

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
</details>

<details class="more">
<summary>Combien de tempêtes placent au moins un département en
orange</summary>

<div class="tablewrap"><table class="wraphead">
<thead><tr>
  <th>Lecture</th><th>Tempêtes<br>(sur évaluées)</th><th>Saisons</th>
  <th>Période de<br>retour (ans)</th>
  <th>Départements<br>par tempête (médiane)</th>
  <th>Préavis médian<br>(h avant passage)</th>
</tr></thead>
<tbody>
{_summary_rows(counts)}
</tbody></table></div>

<p class="legend">
  Période de retour : {N_YEARS} saisons divisées par le nombre de saisons
  avec au moins une tempête concernée. Le préavis est le délai entre la
  première prévision atteignant le critère et le passage au plus près.
</p>
</details>

<details class="more">
<summary>Le détail par département : rafales et pluie prévues</summary>

<p>
  Pour chaque tempête et chaque département, la valeur maximale prévue
  pendant l’approche. Les cases orange atteignent le seuil. Départements
  du sud-ouest au nord-est.
</p>

<h3>Rafales prévues maximales dans le département (km/h)</h3>
{_matrix(verdicts, storms_order, "wind_max_kmh", dc.ORANGE_WIND_KMH[0])}

<h3>Pluie prévue — moyenne du département (mm / jour)</h3>
{_matrix(verdicts, storms_order, "rain_mean_max_mm", thr_mm)}

<h3>Pluie prévue — point le plus arrosé du département (mm / jour)</h3>
{_matrix(verdicts, storms_order, "rain_pix_max_mm", thr_mm)}
</details>

<details class="more">
<summary>Préavis, et fréquence par département</summary>

<h3>Préavis</h3>
<p>
  Pour les tempêtes ayant placé au moins un département en orange : heures
  entre la première prévision atteignant le critère (dans n’importe quel
  département) et le passage au plus près. Une valeur négative (en rouge)
  signifie que le seuil n’a été prévu qu’après le passage — l’alerte
  aurait été tardive.
</p>
{_lead_table(counts)}

<h3>Fréquence par département</h3>
<p>
  Nombre de tempêtes ayant placé chaque département en orange sur
  {dc.SEASON_START}–{dc.SEASON_END}, et période de retour correspondante.
</p>
{_freq_table(freq)}
</details>
"""


def _red_section(pdf, fl, warnings, verdicts, hw_chart):
    # Storms the mechanism activates without the red + HW pathway.
    others = fl[list(OTHER_PATHWAYS)].fillna(False).any(axis=1)
    acts = set(pdf.loc[others, "atcf_id"])
    labels = pdf.set_index("atcf_id")["label"]
    pre = warnings[warnings["pre_cutoff"]].sort_values(
        "time_to_closest_h", ascending=False
    )
    pre_names = " et ".join(
        f"{_de(labels[a])} ({_h(h)}&nbsp;h)"
        for a, h in zip(pre["atcf_id"], pre["time_to_closest_h"])
    )
    late = warnings[~warnings["pre_cutoff"]]
    lo, hi = late["time_to_closest_h"].min(), late["time_to_closest_h"].max()
    # Counted before the cutoff by the replay although the advisory's own
    # forecast lead is under it (Isaac 2012).
    edge = pre[pre["time_to_closest_h"] < LT_CUTOFF_HRS]
    edge_note = " ".join(
        f"{labels[a]} est un cas limite : son avis prévoyait le passage à "
        f"{_h(h)}&nbsp;h, mais le rejeu de calibration le compte avant "
        "l’heure limite ; la tempête active de toute façon le cadre."
        for a, h in zip(edge["atcf_id"], edge["time_to_closest_h"])
    )
    by_rain = all(
        bool(fl.loc[pdf["atcf_id"] == a, "fcast_rain"].any())
        for a in pre["atcf_id"]
    )
    how = "par les précipitations prévues" if by_rain else "par d’autres voies"
    # What the pathway would add if it were exempt from the cutoff.
    extra = warnings[~warnings["atcf_id"].isin(acts)]
    pops = pdf.set_index("atcf_id")["pop_affected_n"]
    extra_list = [
        f"{labels[a]} ({fr_num(pops.get(a))}"
        + (" personnes affectées)" if i == 0 else ")")
        for i, a in enumerate(extra["atcf_id"])
    ]
    extra_names = (
        ", ".join(extra_list[:-1]) + " et " + extra_list[-1]
        if len(extra_list) > 1
        else "".join(extra_list)
    )
    extra_li = ""
    if len(extra):
        e_lo = extra["time_to_closest_h"].min()
        e_hi = extra["time_to_closest_h"].max()
        extra_li = (
            "<li>Si la voie était exemptée de l’heure limite, le Hurricane "
            f"Warning ajouterait {extra_names}, dont les Hurricane Warnings "
            f"sont arrivés {_h(e_lo)} à {_h(e_hi)} heures avant le passage "
            "prévu.</li>"
        )

    rows = []
    by_lead = warnings.sort_values("time_to_closest_h", ascending=False)
    for w in by_lead.itertuples():
        lead = w.time_to_closest_h
        rows.append(
            "<tr>"
            f"<td class='nm'>{escape(str(labels[w.atcf_id]))}</td>"
            f"<td style='white-space:normal'>{escape(w.area_fr)}</td>"
            f"<td>{w.warning_issued_utc.day} "
            f"{MONTHS_FR[w.warning_issued_utc.month]} "
            f"{w.warning_issued_utc:%H}&nbsp;h</td>"
            f"<td class='val{' pre' if w.pre_cutoff else ''}'>"
            f"{_h(lead)}&nbsp;h</td>"
            f"<td class='num'>{_h(w.actual_lead_h)}&nbsp;h</td>"
            f"<td>{'oui' if w.pre_cutoff else 'non'}</td>"
            f"<td>{'oui' if w.atcf_id in acts else 'non'}</td>"
            "</tr>"
        )

    gust = verdicts.groupby("atcf_id")["wind_max_kmh"].max()
    red_gust = gust[gust > 200]
    red_names = _and(
        [f"{labels.get(a, a)} ({fr_num(v)} km/h)" for a, v in red_gust.items()]
    )
    red_tail = plural(len(red_gust), "qui active", "qui activent")
    red_new = [a for a in red_gust.index if a not in acts]
    if red_new:
        raise RuntimeError(f"red gust threshold adds storms: {red_new}")

    return f"""
<p class="answer">
  Tenue à l’heure limite, cette voie n’ajoute aucune activation sur
  {dc.SEASON_START}–{dc.SEASON_END}. Elle reste dans le cadre pour une
  tempête exceptionnelle que la DGPC et le NHC signaleraient tous deux plus
  de {LT_CUTOFF_HRS} heures avant son passage.
</p>

<p>
  La condition exige deux choses <b>avant l’heure limite</b>, c’est-à-dire
  plus de {LT_CUTOFF_HRS} heures avant le passage prévu : une alerte rouge
  de la DGPC <i>et</i> un Hurricane Warning du NHC pour Haïti. La DGPC ne
  dispose pas d’un historique de ses alertes ; on peut en revanche rejouer
  le Hurricane Warning, et confronter l’alerte rouge à la définition qu’en
  donne la DGPC.
</p>

<h3>Le Hurricane Warning du NHC</h3>

<p>
  Un Hurricane Warning annonce des conditions d’ouragan sur la zone
  concernée ; le NHC l’émet en principe 36 heures avant l’arrivée attendue
  des vents de force tempête tropicale, donc le plus souvent à moins de
  {LT_CUTOFF_HRS} heures du passage. Entre {dc.SEASON_START} et
  {dc.SEASON_END}, <b>{len(warnings)} tempêtes</b> ont porté un Hurricane
  Warning pour une partie d’Haïti. Seuls ceux {pre_names} ont été émis
  sur un avis antérieur à l’heure limite, et
  {plural(len(pre), "cette tempête active", "ces tempêtes activent")} déjà
  le cadre {how}. Les {len(late)} autres
  sont arrivés {_h(lo)} à {_h(hi)} heures avant le passage prévu, après
  l’heure limite.
</p>

<figure>{hw_chart}<figcaption>Heures entre l’avis du NHC portant le
Hurricane Warning pour Haïti et le passage au plus près qu’il prévoyait.
L’heure limite est à {LT_CUTOFF_HRS} h.</figcaption></figure>

<div class="tablewrap"><table class="wraphead">
<thead><tr><th>Tempête</th><th>Zone sous Hurricane Warning</th>
<th>Émis (UTC)</th><th>Passage prévu</th><th>Passage réel</th>
<th>Avant l’heure<br>limite</th><th>Cadre activé<br>par une autre voie</th>
</tr></thead>
<tbody>
{chr(10).join(rows)}
</tbody></table></div>

<p class="legend">
  Source : tableau des veilles et avertissements du rapport de cyclone
  tropical (TCR) du NHC de chaque tempête. Passage prévu : heures entre
  l’avis portant l’avertissement et le passage au plus près qu’il prévoyait
  — la mesure qu’applique le système de suivi pour l’heure limite. Passage
  réel : heures entre l’émission et le passage effectivement observé.
  {edge_note}
</p>

<h3>L’alerte rouge de la DGPC</h3>

<p>
  Le Plan SAPMAH Cyclone (2025) place l’alerte rouge tard, par
  construction : « au moins 24 heures, max 2 jours » avant l’impact
  (tableau 7, p. 30), et, en cas de menace extrême, « au moins 36 heures
  avant l’impact attendu » (p. 29). Une alerte rouge émise plus de
  {LT_CUTOFF_HRS} heures avant le passage sortirait donc de la pratique
  décrite par le plan. Ses seuils (p. 35) — rafales de plus de 200 voire
  250 km/h, pluies de plus de 60 à 80 mm en une heure ou 300 mm en 6 à
  12 heures — ne sont atteints, en rafales prévues avant l’heure limite,
  {f"que par {red_names}, {red_tail} déjà le cadre" if len(red_gust)
    else "par aucune tempête"}.
</p>

<div class="note">
  <h3>En résumé</h3>
  <ul>
    <li>Les deux moitiés de la condition arrivent, en règle générale, à
        moins de {LT_CUTOFF_HRS} heures du passage : l’alerte rouge par la
        règle de la DGPC, le Hurricane Warning par celle du NHC.</li>
    <li>Dans l’historique, aucune tempête n’aurait été activée par cette
        seule voie : le registre et les périodes de retour du cadre sont
        les mêmes avec ou sans elle.</li>
    {extra_li}
  </ul>
</div>
"""


def _method_section(pdf, fl, warnings):
    labels = pdf.set_index("atcf_id")["label"]
    others = fl[list(OTHER_PATHWAYS)].fillna(False).any(axis=1)
    acts = set(pdf.loc[others, "atcf_id"])
    missed = warnings[~warnings["atcf_id"].isin(HAND_CODED_HW)]
    changes = [labels[a] for a in missed["atcf_id"] if a not in acts]
    change_txt = (
        f"Seul{'s' if len(changes) > 1 else ''} {_and(changes)} "
        f"{plural(len(changes), 'change', 'changent')} le résultat, et "
        "seulement si la voie était exemptée de l’heure limite."
        if changes
        else "Aucune ne change le résultat."
    )
    return f"""
<ul>
  <li><b>Tempêtes</b> : centre analysé passé à moins de {D_THRESH} km
      d’Haïti, {dc.SEASON_START}–{dc.SEASON_END}. Le registre des
      diapositives retient aussi des tempêtes plus lointaines ayant causé
      des impacts (Ivan 2004), marquées n/d.</li>
  <li><b>Avis considérés</b> : chaque avis du NHC dont le passage au plus
      près prévu est à {LT_CUTOFF_HRS} h ou plus — l’heure limite des voies
      de prévision. Les indicateurs d’heure limite et de pluie prévue sont
      repris du rejeu historique qui a servi à calibrer les seuils 2026
      (Melissa 2025 : valeurs du registre).</li>
  <li><b>Prévisions de pluie</b> : pour chaque avis, la prévision
      CHIRPS-GEFS du jour (ou la plus récente), sur les jours où la
      trajectoire prévue passe à moins de {D_THRESH} km d’Haïti. Produit
      <b>journalier</b> : la valeur d’un jour calendaire tient lieu du
      cumul sur 24 h pour l’alerte orange, ce qui sous-estime légèrement un
      cumul glissant. Moyennes départementales sur un ré-échantillonnage à
      {dc.GEFS_UPSAMPLE_RES}°.</li>
  <li><b>Prévisions de vent</b> : champ de vent paramétrique reconstruit à
      partir des rayons de vent de chaque avis du NHC, vent soutenu réduit
      sur terre (×0,85) puis converti en rafales (×1,25, facteur
      conventionnel en terrain dégagé), maximum dans chaque département.
      Méthode et validation dans l’évaluation en annexe.</li>
  <li><b>Exposition</b> : population dans le polygone de vents de
      64 nœuds prévu, hors bande déjà observée, plus la population de
      cette bande ; en observation, la population de la bande observée.</li>
  <li><b>Alerte orange simulée</b> : un département est compté dès qu’un
      avis émis avant l’heure limite atteint le critère, quelle que soit
      l’échéance. La lecture en rafales est plus permissive que celle en
      vent soutenu : Chantal 2013, simple tempête tropicale passée au
      large, place ainsi les dix départements en orange sur ses prévisions
      à cinq jours.</li>
  <li><b>Hurricane Warnings</b> : liste vérifiée contre le rapport de
      cyclone tropical du NHC de chacune des tempêtes du jeu ; elle compte
      {len(warnings)} tempêtes, {len(missed)} de plus que la liste utilisée
      lors de la première calibration
      ({_and([labels[a] for a in missed["atcf_id"]])}). {change_txt}</li>
  <li><b>Les alertes de la DGPC sont simulées</b> : à défaut de registre,
      les seuils de la DGPC sont appliqués aux prévisions de l’époque. Le
      résultat est une estimation de ce que ces seuils auraient donné, à
      vérifier sur les alertes réellement émises si elles deviennent
      disponibles.</li>
  <li><b>24 saisons</b> est un historique court : une ou deux tempêtes de
      plus ou de moins déplacent sensiblement les périodes de retour.</li>
</ul>
"""


def render(
    tbl,
    verdicts,
    counts,
    freq,
    pdf,
    warnings,
    meta,
    chart="",
    hw_chart="",
    notes=None,
):
    """Assemble the page.

    ``tbl`` is the storm set joined to the deck's activation record;
    ``verdicts`` the storm x department rows; ``counts`` and ``freq`` the
    per-storm and per-department summaries; ``pdf`` the per-storm pathway
    values (``pathways.parquet`` with labels, CERF and impact);
    ``warnings`` the Hurricane Warnings with their cutoff status; ``meta``
    the map thresholds.
    """
    today = date.today()
    stamp = f"{MONTHS_FR[today.month]} {today.year}"
    notes = notes or {}

    fl = stage_flags(pdf, warnings)
    check_record(pdf, fl)
    n_act = int(fl["all"].sum())
    n_seasons = int(pdf.loc[fl["all"], "season"].nunique())

    return f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cadre 2026 — ouragans, Haïti</title>
<meta name="description" content="Le mécanisme de déclenchement 2026 du
cadre d’action anticipatoire pour les tempêtes et ouragans en Haïti, et les
analyses qui l’ont fondé.">
<style>{CSS}{MAP_CSS}{EXTRA_CSS}</style>
</head>
<body>
<div class="wrap wide">

<header>
  <a class="home-link" href="../">← Action anticipatoire · Ouragans · Haïti</a>
  <div class="kicker">Cadre d’action anticipatoire · Version 2026</div>
  <h1>Tempêtes et ouragans en Haïti : le mécanisme de déclenchement 2026</h1>
  <p class="sub">
    Le mécanisme de déclenchement approuvé pour la version 2026 du cadre,
    et l’ensemble des analyses qui l’ont fondé : la simulation historique
    sur {dc.SEASON_START}–{dc.SEASON_END}, l’alerte orange de la DGPC par
    département, l’alerte rouge confirmée par un Hurricane Warning du NHC,
    la carte avis par avis et les diapositives. Préparé par le Centre de
    données humanitaires d’OCHA avec la DGPC et les partenaires du cadre.
  </p>
</header>

<nav class="toc">
  <b>Sommaire</b>
  <ol>
    <li><a href="#declencheur">Le déclencheur</a></li>
    <li><a href="#diapositives">Les diapositives</a></li>
    <li><a href="#simulation">Simulation historique, avis par avis</a></li>
    <li><a href="#seuils">Le choix des seuils</a></li>
    <li><a href="#orange">L’alerte orange de la DGPC par département</a></li>
    <li><a href="#rouge">L’alerte rouge de la DGPC et le Hurricane
        Warning</a></li>
    <li><a href="#methode">Méthode et limites</a></li>
    <li><a href="#annexe">Annexe</a></li>
  </ol>
</nav>

<h2 id="declencheur">1. Le déclencheur</h2>
{_definition_section(n_act, n_seasons)}

<h2 id="diapositives">2. Les diapositives</h2>
{_deck_section()}

<h2 id="simulation">3. Simulation historique, avis par avis</h2>
{_map_section(pdf, meta, warnings)}

<h2 id="seuils">4. Le choix des seuils</h2>
{_thresholds_section(pdf, fl)}

<h2 id="orange">5. L’alerte orange de la DGPC par département</h2>
{_orange_section(tbl, verdicts, counts, freq, pdf, fl, chart, notes)}

<h2 id="rouge">6. L’alerte rouge de la DGPC confirmée par un Hurricane
Warning</h2>
{_red_section(pdf, fl, warnings, verdicts, hw_chart)}

<h2 id="methode">7. Méthode et limites</h2>
{_method_section(pdf, fl, warnings)}

<h2 id="annexe">Annexe</h2>
<ul>
  <li><a href="premiere-evaluation.html"><b>Niveaux d’alerte de la DGPC :
      première évaluation historique</b></a> (août 2026, archivée) — les
      seuils orange et rouge de la DGPC appliqués aux tempêtes depuis 2002
      à trois échelles spatiales, en vent observé comme prévu, avant que la
      DGPC ne précise que ses alertes sont émises par département.</li>
  <li><a href="https://github.com/OCHA-DAP/ds-aa-hti-hurricanes"><b>Code
      source</b></a> du système de suivi et de ces analyses
      (ds-aa-hti-hurricanes).</li>
</ul>

<footer>
  OCHA · Centre de données humanitaires — cadre d’action anticipatoire pour
  les tempêtes et ouragans en Haïti, version 2026. Données : NHC (avis de
  prévision, rayons de vent et rapports de cyclone tropical), CHIRPS-GEFS
  (UCSB CHC), IMERG (NASA), WorldPop 2026, EM-DAT ; Plan SAPMAH Cyclone
  2025 (DGPC). Fond de carte OpenStreetMap. Mise à jour {escape(stamp)}.
</footer>

</div>
{map_js(warnings)}
</body>
</html>
"""
