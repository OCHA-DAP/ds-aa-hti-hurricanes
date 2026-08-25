"""Render the French DGPC analysis page.

Terminology follows the team's doc-sourced French glossary (KB
``docs/I18N.md``): *déclencheur*, *seuil atteint / non atteint*,
*précipitations* for the indicator, *nœuds* spelled out, *période de
retour*, *passage au plus près*.
"""

from datetime import date
from html import escape

import numpy as np
import pandas as pd

from src.constants import D_THRESH
from src.dgpc import constants as dc

MONTHS_FR = {
    1: "janvier",
    2: "février",
    3: "mars",
    4: "avril",
    5: "mai",
    6: "juin",
    7: "juillet",
    8: "août",
    9: "septembre",
    10: "octobre",
    11: "novembre",
    12: "décembre",
}

CSS = """
:root{--blue:#1862d8;--blue-dark:#0e4aab;--ink:#1e2a2b;--muted:#5e6a6b;
--bg:#f4f7f9;--card:#fff;--line:#dde5e8;--orange:#ec835a;--red:#d03b3b;
--green:#0f8a5f;--amber-bg:#fdf6ec;--amber-line:#e8cfa8;--amber-ink:#8a5a12}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,
"Helvetica Neue",Arial,sans-serif;color:var(--ink);background:var(--bg);
line-height:1.62;padding:0 5vw 6rem}
.wrap{max-width:56rem;margin:0 auto}
header{padding:4.5rem 0 2.2rem}
.kicker{text-transform:uppercase;letter-spacing:.14em;font-size:.78rem;
font-weight:700;color:var(--blue);margin-bottom:.8rem}
h1{font-size:clamp(1.7rem,4.4vw,2.5rem);line-height:1.16;margin-bottom:1rem}
.sub{font-size:1.08rem;color:var(--muted);max-width:44em}
.crumb{font-size:.86rem;margin-bottom:1.6rem}
.crumb a{color:var(--muted)}
h2{font-size:1.42rem;margin:3.2rem 0 .9rem;line-height:1.25;
padding-top:1.6rem;border-top:1px solid var(--line)}
h3{font-size:1.08rem;margin:1.9rem 0 .5rem}
p{margin-bottom:.95rem;max-width:44em}
ul,ol{margin:0 0 1rem 1.15rem;max-width:44em}
li{margin-bottom:.42rem}
code{background:#eef2f5;padding:.1em .38em;border-radius:4px;font-size:.9em}
.lead{font-size:1.05rem}
.callout{background:var(--amber-bg);border:1px solid var(--amber-line);
border-left:4px solid var(--orange);border-radius:8px;padding:1.1rem 1.3rem;
margin:1.5rem 0;max-width:44em}
.callout h3{margin-top:0;color:var(--amber-ink);font-size:1rem}
.callout p:last-child{margin-bottom:0}
.note{background:#fff;border:1px solid var(--line);border-radius:8px;
padding:1.1rem 1.3rem;margin:1.5rem 0;max-width:44em;font-size:.94rem}
.note h3{margin-top:0;font-size:1rem}
.note p:last-child{margin-bottom:0}
figure{margin:1.8rem 0;background:var(--card);border:1px solid var(--line);
border-radius:10px;padding:1.2rem 1rem .8rem;overflow-x:auto}
figcaption{font-size:.86rem;color:var(--muted);margin-top:.7rem;
padding:0 .4rem}
.tablewrap{overflow-x:auto;margin:1.4rem 0;border:1px solid var(--line);
border-radius:10px;background:var(--card)}
table{border-collapse:collapse;width:100%;font-size:.9rem}
th,td{padding:.5rem .7rem;text-align:left;border-bottom:1px solid var(--line);
white-space:nowrap;vertical-align:top}
table.prose th,table.prose td{white-space:normal;min-width:11rem}
table.wraphead th{white-space:normal}
th{background:#f7fafb;font-weight:700;font-size:.82rem;color:var(--muted);
text-transform:uppercase;letter-spacing:.04em;position:sticky;top:0}
tr:last-child td{border-bottom:none}
td.num{text-align:right;font-variant-numeric:tabular-nums}
td.nm{font-weight:600}
.v-yes{color:var(--green);font-weight:600}
.v-no{color:var(--muted)}
.v-na{color:var(--muted)}
.pill{display:inline-block;font-size:.72rem;text-transform:uppercase;
letter-spacing:.08em;font-weight:700;padding:.18rem .5rem;border-radius:5px}
.pill.orange{background:#fdefe7;color:#a4551f}
.pill.red{background:#fbeaea;color:#a02a2a}
.stats{display:grid;gap:1rem;grid-template-columns:repeat(auto-fit,minmax(11rem,1fr));
margin:1.6rem 0}
.stat{background:var(--card);border:1px solid var(--line);border-radius:10px;
padding:1rem 1.1rem}
.stat .big{font-size:1.9rem;font-weight:700;line-height:1.1;
font-variant-numeric:tabular-nums}
.stat .lab{font-size:.83rem;color:var(--muted);margin-top:.3rem}
footer{margin-top:3.5rem;padding-top:1.6rem;border-top:1px solid var(--line);
font-size:.86rem;color:var(--muted)}
footer a{color:var(--muted)}
"""


def fr_num(v, decimals=0):
    if v is None or (isinstance(v, (int, float)) and not np.isfinite(v)):
        return "—"
    return f"{v:,.{decimals}f}".replace(",", " ").replace(".", ",")


def verdict(flag):
    if flag is None or (np.isscalar(flag) and pd.isna(flag)):
        return '<span class="v-na">—</span>'
    return (
        '<span class="v-yes">✓ atteint</span>'
        if bool(flag)
        else '<span class="v-no">— non atteint</span>'
    )


def plural(n, singular, plural_form=None):
    """French agreement: 0 and 1 take the singular."""
    return singular if abs(n) < 2 else (plural_form or singular + "s")


def _rp_tile(rp_years, n_events, label):
    """A return-period stat tile, flagged when built on too few events."""
    if not np.isfinite(rp_years):
        big, caveat = "jamais", ""
    else:
        big = f"{fr_num(rp_years, 1)} ans"
        caveat = (
            f" <b>— estimation fragile : {n_events} "
            f"{plural(n_events, 'événement')}</b>"
            if n_events < 3
            else ""
        )
    return (
        f'<div class="stat"><div class="big">{big}</div>'
        f'<div class="lab">{label}{caveat}</div></div>'
    )


def render(df, chart_wind, rp, rain, variant, rmw_note):
    """Assemble the whole page."""
    v = variant
    n_storms = len(df)
    today = date.today()
    stamp = f"{MONTHS_FR[today.month]} {today.year}"
    n_years = dc.SEASON_END - dc.SEASON_START + 1

    obsv_orange = df[f"obsv_{v}_orange"].fillna(False).astype(bool)
    obsv_red = df[f"obsv_{v}_red"].fillna(False).astype(bool)

    n_o, n_r = int(obsv_orange.sum()), int(obsv_red.sum())
    yrs_o = sorted(df.loc[obsv_orange, "season"].unique())
    yrs_r = sorted(df.loc[obsv_red, "season"].unique())
    rp_o = (n_years + 1) / len(yrs_o) if yrs_o else np.inf
    rp_r = (n_years + 1) / len(yrs_r) if yrs_r else np.inf

    orange_storms = df.loc[obsv_orange, "label"].tolist()
    red_storms = df.loc[obsv_red, "label"].tolist()

    rows = []
    for _, r in df.iterrows():
        rows.append(
            "<tr>"
            f"<td class='nm'>{escape(r['label'])}</td>"
            f"<td class='num'>{fr_num(r['min_dist_km'])}</td>"
            f"<td class='num'>{fr_num(r.get(f'obsv_{v}_max_kmh'))}</td>"
            f"<td class='num'>{fr_num(r.get(f'{v}_max_kmh'))}</td>"
            f"<td>{verdict(r.get(f'obsv_{v}_orange'))}</td>"
            f"<td>{verdict(r.get(f'obsv_{v}_red'))}</td>"
            f"<td class='num'>{fr_num(r.get(f'obsv_{v}_pop_orange'))}</td>"
            "</tr>"
        )
    storm_rows = "\n".join(rows)

    rp_rows = "\n".join(
        f"<tr><td class='nm'>{escape(r.criterion)}</td>"
        f"<td class='num'>{r.n_storms}</td>"
        f"<td class='num'>{r.n_years}</td>"
        f"<td class='num'>{fr_num(r.rp_years, 1)}</td>"
        f"<td style='white-space:normal'>{escape(r.years) or '—'}</td></tr>"
        for r in rp.itertuples()
    )

    rain_section = _rain_section(rain)
    sens_rows = _sensitivity_rows(df)

    return f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Niveaux d’alerte de la DGPC — évaluation historique</title>
<style>{CSS}</style>
</head>
<body>
<div class="wrap">

<header>
  <p class="crumb"><a href="./">←
  Action anticipatoire · Ouragans · Haïti</a></p>
  <div class="kicker">Analyse · {escape(stamp)}</div>
  <h1>Niveaux d’alerte de la DGPC : évaluation historique</h1>
  <p class="sub">
    Les seuils de pluie et de vent des alertes orange et rouge communiqués par
    la DGPC en août 2026, appliqués rétrospectivement aux {n_storms} tempêtes
    passées à moins de {D_THRESH} km d’Haïti entre
    {dc.SEASON_START} et {dc.SEASON_END}.
  </p>
</header>

<p class="lead">
  La DGPC a communiqué les seuils qui définissent ses niveaux d’alerte orange
  et rouge. Cette page répond à une seule question : <strong>quelles tempêtes
  passées auraient atteint ces seuils ?</strong> La réponse dépend fortement
  d’hypothèses que la DGPC n’a pas précisées — elles sont listées à la
  section 2 et devraient être tranchées avant toute utilisation de ces
  seuils dans le cadre d’action anticipatoire.
</p>

<div class="stats">
  <div class="stat"><div class="big">{n_o}</div>
    <div class="lab">{plural(n_o, "tempête")} atteignant
    le seuil de vent <b>orange</b>
    (≥ 100 km/h sur terre, observation)</div></div>
  <div class="stat"><div class="big">{n_r}</div>
    <div class="lab">{plural(n_r, "tempête")} atteignant
    le seuil de vent <b>rouge</b>
    (≥ 200 km/h sur terre, observation)</div></div>
  {_rp_tile(rp_o, len(yrs_o), "période de retour du seuil de vent orange")}
  {_rp_tile(rp_r, len(yrs_r), "période de retour du seuil de vent rouge")}
</div>

<div class="callout">
  <h3>Statut : analyse préliminaire</h3>
  <p>
    Le volet <b>vent</b> est complet. Le volet <b>précipitations</b> est en
    attente : il exige des données de pluie infra-journalières (les seuils
    de 60–80 mm/h et de 300 mm/6–12 h ne peuvent pas être évalués sur des
    cumuls journaliers), et l’accès au produit IMERG semi-horaire n’était pas
    disponible au moment de la rédaction. La méthode est prête et documentée
    à la section 5.
  </p>
</div>

<h2>1. Ce que la DGPC a communiqué</h2>

<div class="tablewrap"><table>
<thead><tr><th>Niveau</th><th>Précipitations</th><th>Vent</th></tr></thead>
<tbody>
<tr><td><span class="pill orange">Orange</span></td>
    <td>100 mm / 24 h</td><td>100–120 km/h</td></tr>
<tr><td><span class="pill red">Rouge</span></td>
    <td>60–80 mm/h <i>ou</i> 300 mm / 6–12 h</td><td>≥ 200 km/h</td></tr>
</tbody></table></div>

<p>
  Converties en nœuds — l’unité des prévisions du NHC — les vitesses de vent
  correspondent à environ <b>54 nœuds</b> (100 km/h), <b>65 nœuds</b>
  (120 km/h) et <b>108 nœuds</b> (200 km/h). Aucune de ces valeurs ne
  correspond à un rayon de vent prévu par le NHC, qui ne publie que les
  rayons à 34, 50 et 64 nœuds ; la section 4 explique comment le champ de
  vent est reconstruit.
</p>

<h2>2. Ce que nous avons dû supposer</h2>

<p>
  La DGPC n’a précisé ni la nature des valeurs (prévues ou observées), ni les
  sources de données, ni la surface sur laquelle un seuil doit être atteint.
  Chaque hypothèse ci-dessous change le résultat, parfois du tout au tout.
</p>

<div class="tablewrap"><table class="prose">
<thead><tr><th>Question ouverte</th><th>Hypothèse retenue ici</th>
<th>Ce que cela change</th></tr></thead>
<tbody>
<tr><td>Prévision ou observation ?</td>
    <td>Les deux sont présentées côte à côte pour le vent</td>
    <td>L’écart est décisif : voir Matthew ci-dessous</td></tr>
<tr><td>Vent en mer ou sur terre ?</td>
    <td>Sur terre, avec réduction de frottement (×0,85)</td>
    <td>Environ 15 % — assez pour franchir ou non le seuil rouge</td></tr>
<tr><td>Vent soutenu ou rafale ?</td>
    <td>Vent soutenu (résultat principal) ; rafales en sensibilité</td>
    <td>Environ 25 % de plus pour les rafales</td></tr>
<tr><td>Sur quelle surface ?</td>
    <td>Pour les précipitations : moyenne nationale, moyenne départementale
        et point de grille, présentées côte à côte. Pour le vent : maximum
        atteint en un point du territoire, avec le détail par département —
        une « moyenne nationale de vent » n’aurait pas de sens physique.</td>
    <td>Le seuil de pluie peut être atteint localement sans l’être
        en moyenne nationale ; c’est le choix le plus lourd de
        conséquences de toute la liste</td></tr>
<tr><td>« 60–80 mm/h » : quelle borne ?</td>
    <td>La borne basse, 60 mm en 1 h</td>
    <td>Lecture la plus permissive</td></tr>
<tr><td>« 300 mm / 6–12 h » : quelle fenêtre ?</td>
    <td>300 mm en 12 h (le résultat à 6 h est aussi donné)</td>
    <td>La fenêtre de 6 h est nettement plus exigeante</td></tr>
<tr><td>« 100–120 km/h » : plage ou seuil ?</td>
    <td>Seuil minimal : ≥ 100 km/h</td>
    <td>Une lecture en plage fermée exclurait les tempêtes les plus
        fortes</td></tr>
</tbody></table></div>

<h2>3. Tempêtes considérées</h2>

<p>
  Les {n_storms} tempêtes dont le centre analysé est passé à moins de
  {D_THRESH} km
  d’Haïti entre {dc.SEASON_START} et {dc.SEASON_END} — le même critère de
  distance que celui utilisé pour attribuer les précipitations à une tempête
  dans le cadre. La période commence en {dc.SEASON_START} parce que c’est
  l’année où le NHC a commencé à prévoir les rayons de vent à 64 nœuds, sans
  lesquels le champ de vent ne peut pas être reconstruit.
</p>

<h2>4. Méthode — vent</h2>

<p>
  Le NHC ne publie que les rayons de vent par quadrant à 34, 50 et 64 nœuds.
  Les niveaux de la DGPC (54, 65 et 108 nœuds) n’en font pas partie : un
  champ de vent paramétrique est donc reconstruit pour chaque point de la
  trajectoire.
</p>

<ul>
  <li><b>Profil radial.</b> Loi de puissance par morceaux passant exactement
      par les points d’ancrage (R<sub>max</sub>, V<sub>max</sub>),
      (r<sub>64</sub>, 64), (r<sub>50</sub>, 50), (r<sub>34</sub>, 34), en
      échelle logarithmique. Le profil reproduit donc chaque rayon publié par
      le NHC à 0,1 nœud près.</li>
  <li><b>Rayon des vents maximaux.</b> Le NHC ne le prévoit pas (le champ est
      systématiquement nul). Il est estimé par la relation climatologique de
      Willoughby et al. (2006). {rmw_note}</li>
  <li><b>Asymétrie.</b> Les rayons par quadrant sont interpolés de manière
      continue en azimut, ce qui conserve l’asymétrie prévue par le NHC.</li>
  <li><b>Passage sur terre.</b> La trajectoire est interpolée à l’heure, le
      champ de vent est évalué sur la grille WorldPop 1 km d’Haïti, et l’on
      retient le maximum atteint en chaque point.</li>
  <li><b>Réduction sur terre.</b> Les rayons du NHC décrivent un vent soutenu
      sur mer ; un facteur de 0,85 est appliqué sur terre, et un facteur de
      rafale de 1,25 pour la variante « rafales ».</li>
</ul>

<h2>5. Méthode — précipitations
<span class="pill orange">en attente</span></h2>

<p>
  Les seuils de la DGPC portent sur des durées de 1 h, 6–12 h et 24 h. Le
  cadre suit aujourd’hui les précipitations au pas <i>journalier</i>
  (CHIRPS-GEFS en prévision, IMERG en observation), ce qui ne permet pas
  d’évaluer les critères infra-journaliers. L’analyse utilise donc le produit
  <b>IMERG semi-horaire</b> (0,1°, pas de 30 minutes), réduit pour chaque
  tempête au cumul glissant maximal sur chaque fenêtre, selon les trois
  agrégations spatiales.
</p>

<div class="note">
  <h3>Un seuil qui ne peut pas être prévu</h3>
  <p>
    Le critère rouge de <b>60–80 mm/h</b> est un critère de <i>mesure</i>, pas
    de prévision : aucun modèle météorologique global (maille de 25 km) ne
    résout une intensité horaire ponctuelle de cet ordre. Il peut être
    constaté en temps réel, mais il ne peut pas déclencher une action
    anticipatoire. Les critères 100 mm/24 h et 300 mm/12 h, eux, sont
    prévisibles.
  </p>
</div>

{rain_section}

<h2>6. Résultats — vent</h2>

<figure>
{chart_wind}
<figcaption>
  Vent soutenu maximal atteint sur le territoire haïtien, par tempête, en
  observation (trajectoire réelle) et en prévision (meilleure émission du
  NHC, toutes échéances confondues). Réduction de frottement de 0,85
  appliquée. Les tempêtes sans barre de prévision n’ont pas d’émission du NHC
  approchant suffisamment Haïti.
</figcaption>
</figure>

<p>
  {_headline(n_o, n_r, orange_storms, red_storms)}
</p>

<div class="tablewrap"><table class="wraphead">
<thead><tr>
  <th>Tempête</th><th>Distance min.<br>(km)</th>
  <th>Vent max.<br>obs. (km/h)</th><th>Vent max.<br>prév. (km/h)</th>
  <th>Seuil<br>orange</th><th>Seuil<br>rouge</th>
  <th>Population<br>≥ 100 km/h</th>
</tr></thead>
<tbody>
{storm_rows}
</tbody></table></div>

<h2>7. Sensibilité : quelle lecture du vent ?</h2>

<p>
  La DGPC n’a pas précisé si « ≥ 200 km/h » désigne un vent soutenu ou une
  rafale, ni s’il s’agit du vent sur terre ou en mer. Les trois lectures
  sont données ci-dessous pour les tempêtes les plus fortes. <b>Le verdict
  sur le niveau rouge change selon la lecture retenue</b> : c’est la
  question la plus urgente à trancher avec la DGPC.
</p>

<div class="tablewrap"><table class="wraphead">
<thead><tr>
  <th>Tempête</th>
  <th>Vent soutenu sur terre (×0,85)</th>
  <th>Vent soutenu non réduit</th>
  <th>Rafale sur terre (×1,25)</th>
  <th>Prévision (soutenu sur terre)</th>
</tr></thead>
<tbody>
{sens_rows}
</tbody></table></div>

<p>
  Toutes les valeurs sont en km/h ; la ligne de 200 km/h est le seuil rouge.
  La lecture « vent soutenu sur terre » est celle retenue comme résultat
  principal, parce que c’est la formulation la plus proche de ce que la DGPC
  a écrit — mais c’est aussi la plus stricte des trois.
</p>

<h2>8. Périodes de retour</h2>

<p>
  Calculées par la position de Weibull sur {n_years} saisons
  ({dc.SEASON_START}–{dc.SEASON_END}) : période de retour =
  ({n_years} + 1) / nombre d’années comptant au moins une activation.
</p>

<div class="tablewrap"><table>
<thead><tr><th>Critère</th><th>Tempêtes</th><th>Années</th>
<th>Période de retour (ans)</th><th>Années concernées</th></tr></thead>
<tbody>
{rp_rows}
</tbody></table></div>

<div class="callout">
  <h3>Ce que cela implique pour le cadre</h3>
  <p>
    Le cadre est calibré autour d’une période de retour globale de
    <b>2,4 ans</b>. Le seuil de vent orange, en observation, ne se produit
    qu’une fois tous les {fr_num(rp_o, 1)} ans, et le seuil rouge une fois
    tous les {fr_num(rp_r, 1)} ans. <b>Les seuils de vent de la DGPC sont
    donc bien trop rares pour piloter à eux seuls un déclencheur d’action
    anticipatoire</b> : utilisés seuls, ils laisseraient passer la grande
    majorité des saisons où le cadre doit agir.
  </p>
  <p>
    Deux conséquences. D’abord, si les niveaux d’alerte de la DGPC doivent
    être reliés au cadre, ce sont les critères de <b>précipitations</b> qui
    porteront l’essentiel des activations — d’où l’importance de compléter
    ce volet. Ensuite, le rapprochement entre les deux systèmes est
    probablement à concevoir comme une <b>correspondance</b> (le cadre
    signale à la DGPC que ses seuils sont en passe d’être atteints) plutôt
    que comme une substitution.
  </p>
</div>

<div class="note">
  <h3>Prudence sur les valeurs rares</h3>
  <p>
    Un enregistrement de {n_years} saisons ne peut pas soutenir une
    affirmation solide au-delà d’environ 1 fois tous les 8 ans. Les périodes
    de retour du niveau rouge reposent sur très peu d’événements et doivent
    être lues comme des ordres de grandeur.
  </p>
</div>

<h2>9. Questions pour la DGPC</h2>

<ol>
  <li>Les seuils s’appliquent-ils à des valeurs <b>prévues</b> ou
      <b>observées</b> ? Le cadre d’action anticipatoire a besoin de valeurs
      prévues ; une alerte fondée sur l’observation arrive après l’impact.</li>
  <li>Les vitesses de vent désignent-elles un <b>vent soutenu</b> ou des
      <b>rafales</b>, et sur terre ou en mer ? L’écart entre ces lectures
      dépasse 40 %.</li>
  <li>Sur quelle <b>surface</b> un seuil de pluie doit-il être atteint : un
      point, une commune, un département, le pays ?</li>
  <li>Le critère de <b>60–80 mm/h</b> a-t-il vocation à déclencher une
      alerte anticipée ? Il n’est pas prévisible ; s’il doit servir, ce ne
      peut être qu’en constat temps réel.</li>
  <li>« 100–120 km/h » désigne-t-il une <b>plage</b> (et donc un plafond au
      delà duquel on passe en rouge) ou un <b>seuil minimal</b> ?</li>
  <li>Quelles <b>sources de données</b> la DGPC utilise-t-elle ou
      souhaite-t-elle voir utilisées ?</li>
</ol>

<h2>10. Limites</h2>

<ul>
  <li>Le champ de vent est un modèle paramétrique, non une simulation : il
      reproduit fidèlement les rayons publiés par le NHC mais interpole entre
      eux, et <b>extrapole</b> vers le cœur pour le niveau à 108 nœuds.</li>
  <li>Le rayon des vents maximaux n’est pas prévu par le NHC. C’est la
      principale incertitude structurelle du niveau rouge.</li>
  <li>La réduction sur terre (0,85) est une valeur conventionnelle et
      uniforme ; le relief haïtien produit en réalité de fortes variations
      locales, à la hausse comme à la baisse.</li>
  <li>Le volet précipitations n’est pas encore calculé.</li>
  <li>Le critère de distance (230 km) est mesuré en projection Web Mercator,
      comme dans le reste du cadre : à la latitude d’Haïti cela correspond à
      environ 217 km réels. Cette convention est conservée pour rester
      comparable aux autres analyses du cadre.</li>
  <li>IMERG (0,1°, soit environ 11 km) lisse les intensités extrêmes de
      courte durée : les résultats à 1 h seront des estimations basses.</li>
</ul>

<footer>
  OCHA · Centre de données humanitaires — analyse préparée pour la révision
  2026 du cadre d’action anticipatoire pour les tempêtes et ouragans en
  Haïti. Code et méthode :
  <a href="https://github.com/OCHA-DAP/ds-aa-hti-hurricanes"
  >ds-aa-hti-hurricanes</a>.
  Données : NHC (prévisions et trajectoires), IBTrACS, WorldPop 2026,
  IMERG (NASA GPM). Mise à jour {escape(stamp)}.
</footer>

</div>
</body>
</html>
"""


def _headline(n_o, n_r, orange_storms, red_storms):
    """One paragraph stating the wind result in words."""
    o_list = ", ".join(escape(s) for s in orange_storms)
    r_list = ", ".join(escape(s) for s in red_storms)
    if n_o:
        head = (
            f"Sur la trajectoire réelle, <b>{n_o}</b> "
            f"{plural(n_o, 'tempête')} {plural(n_o, 'a', 'ont')} amené des "
            f"vents soutenus d’au moins 100 km/h sur le territoire haïtien "
            f"({o_list})."
        )
    else:
        head = (
            "Aucune tempête n’a amené de vent soutenu d’au moins 100 km/h "
            "sur terre."
        )

    if n_r:
        verb = plural(n_r, "d’entre elles a", "d’entre elles ont")
        tail = f" <b>{n_r}</b> {verb} atteint 200 km/h ({r_list})."
    else:
        tail = " Aucune n’a atteint 200 km/h en vent soutenu sur terre."
    return head + tail


def _sensitivity_rows(df, top_n=8):
    """The three wind readings for the strongest storms."""
    v = dc.PRIMARY_WIND_VARIANT
    d = df.sort_values(f"obsv_{v}_max_kmh", ascending=False).head(top_n)
    out = []
    for _, r in d.iterrows():
        cells = []
        for col in (
            f"obsv_{v}_max_kmh",
            "obsv_sustained_marine_max_kmh",
            "obsv_gust_land_max_kmh",
            f"{v}_max_kmh",
        ):
            val = r.get(col)
            over = np.isfinite(val) if val is not None else False
            over = bool(over) and val >= dc.RED_WIND_KMH
            style = " style='color:#a02a2a;font-weight:700'" if over else ""
            cells.append(f"<td class='num'{style}>{fr_num(val)}</td>")
        out.append(
            f"<tr><td class='nm'>{escape(r['label'])}</td>"
            + "".join(cells)
            + "</tr>"
        )
    return "\n".join(out)


def _rain_section(rain):
    if rain is None or len(rain) == 0:
        return (
            '<div class="note"><h3>Résultats — précipitations</h3>'
            "<p>En attente de l’accès aux données IMERG semi-horaires. "
            "La méthode et le code sont en place "
            "(<code>pipelines/run_dgpc_rain.py</code>) ; le calcul prend "
            "environ une heure une fois l’accès rétabli.</p></div>"
        )
    return ""
