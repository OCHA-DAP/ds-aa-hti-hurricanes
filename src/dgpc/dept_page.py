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

from src.constants import D_THRESH, LT_CUTOFF_HRS
from src.dgpc import constants as dc
from src.dgpc.dept_forecast import READINGS
from src.dgpc.page import CSS, MONTHS_FR, _count_cell, _mark, fr_num, plural
from src.dgpc.pathways import PATHWAYS, flags, rp_table

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


MAP_CSS = """
#carte-wrap{background:var(--card);border:1px solid var(--line);border-radius:10px;
padding:1rem;margin:1.4rem 0}
#carte{height:520px;border-radius:8px;border:1px solid var(--line)}
.ctl{display:flex;flex-wrap:wrap;gap:.6rem 1.2rem;align-items:center;
margin-bottom:.8rem;font-size:.92rem}
.ctl select,.ctl button{font:inherit;padding:.3rem .55rem;border:1px solid var(--line);
border-radius:6px;background:#fff}
.ctl button{cursor:pointer}
.ctl button:disabled{opacity:.4;cursor:default}
.ctl input[type=range]{width:min(360px,100%)}
.ctl label{white-space:nowrap}
.advlab{font-weight:700}
.badge{display:inline-block;font-size:.74rem;font-weight:700;padding:.12rem .5rem;
border-radius:5px;margin-left:.4rem;vertical-align:middle}
.badge.ok{background:#e7f4ee;color:#0f8a5f}
.badge.cut{background:#fbeaea;color:#a02a2a}
.toggles{display:flex;flex-wrap:wrap;gap:.3rem 1.1rem;font-size:.88rem;margin:.6rem 0}
.toggles label{display:inline-flex;align-items:center;gap:.3rem;cursor:pointer}
.sw{display:inline-block;width:.9rem;height:.9rem;border-radius:3px;border:1px solid #0002}
#voies{display:grid;grid-template-columns:repeat(auto-fit,minmax(13rem,1fr));gap:.6rem;
margin-top:.8rem;font-size:.88rem}
#voies .v{border:1px solid var(--line);border-radius:8px;padding:.55rem .75rem;background:#fafcfc}
#voies .v.hit{border-color:#0f8a5f;background:#e7f4ee}
#voies .v.off{opacity:.55}
#voies .v b{display:block;font-size:.8rem;color:var(--muted);text-transform:uppercase;
letter-spacing:.04em;margin-bottom:.15rem}
#voies .v .val{font-size:1.15rem;font-weight:700;font-variant-numeric:tabular-nums}
.leaflet-tooltip.dep{font-size:.82rem;line-height:1.35}
"""

MAP_HTML = """
<div id="carte-wrap">
  <div class="ctl">
    <label>Tempête
      <select id="storm-sel"></select></label>
    <button id="prev" type="button" aria-label="Avis précédent">◀</button>
    <input id="adv-range" type="range" min="0" max="0" value="0">
    <button id="next" type="button" aria-label="Avis suivant">▶</button>
    <button id="play" type="button">▶ Lecture</button>
    <span id="adv-lab" class="advlab"></span>
  </div>
  <div class="toggles">
    <label><input type="checkbox" id="t-wind" checked>
      <span class="sw" style="background:#ec835a"></span> Orange — rafales ≥ 100 km/h</label>
    <label><input type="checkbox" id="t-rain" checked>
      <span class="sw" style="background:#1862d8"></span> Orange — pluie ≥ 100 mm (point)</label>
    <label><input type="checkbox" id="t-cum" checked>
      <span class="sw" style="background:#f7d9c9"></span> Départements déjà en orange (avis précédents, avant l’heure limite)</label>
    <label><input type="checkbox" id="t-track" checked>
      <span class="sw" style="background:#1e2a2b"></span> Trajectoire prévue</label>
    <label><input type="checkbox" id="t-obsv">
      <span class="sw" style="background:#9aa5a6"></span> Trajectoire observée</label>
    <label><input type="checkbox" id="t-voies" checked> Voies du cadre</label>
  </div>
  <div id="carte"></div>
  <div id="voies"></div>
</div>
"""

MAP_JS = r"""
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<script>
(function(){
  const $ = id => document.getElementById(id);
  const fmt = (v, d=0) => (v==null ? '—' : v.toLocaleString('fr-FR',{maximumFractionDigits:d}));
  let DATA, storm, idx = 0, timer = null;
  const map = L.map('carte', {scrollWheelZoom:false}).setView([18.9,-72.8], 7);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',
    {attribution:'&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>', maxZoom:12, opacity:.55}).addTo(map);
  let deptLayer, trackLayer = L.layerGroup().addTo(map), obsvLayer = L.layerGroup();
  const COL = {none:'#e9eef0', cum:'#f7d9c9', wind:'#ec835a', rain:'#1862d8', both:'#8a2a8a', dim:'#d9dfe2'};

  function cumBefore(i){ // departments in orange on pre-cutoff advisories before i
    const s = new Set();
    for (let k=0;k<i;k++){ const a=storm.adv[k]; if(a.cut) continue;
      DATA.meta.depts.forEach((d,j)=>{ if(hit(a,j).any) s.add(d); }); }
    return s;
  }
  function hit(a, j){
    const w = $('t-wind').checked && a.w[j]!=null && a.w[j] >= DATA.meta.wind_kmh;
    const r = $('t-rain').checked && a.rp[j]!=null && a.rp[j] >= DATA.meta.rain_mm;
    return {w, r, any: w||r};
  }
  function render(){
    const a = storm.adv[idx], m = DATA.meta, cum = cumBefore(idx);
    const d = new Date(a.t+'Z');
    $('adv-lab').innerHTML = `Avis du ${d.toLocaleString('fr-FR',{timeZone:'UTC',day:'numeric',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'})} UTC` +
      ` · passage au plus près prévu dans ${fmt(a.ttc)} h` +
      (a.cut ? '<span class="badge cut">après l’heure limite</span>' : '<span class="badge ok">avant l’heure limite</span>') +
      ` <span style="color:#5e6a6b;font-weight:400">(${idx+1}/${storm.adv.length})</span>`;
    // departments
    let nNow = 0; const cumNow = new Set(cum);
    deptLayer.eachLayer(l => {
      const j = m.depts.indexOf(l.feature.properties.name); const h = hit(a,j);
      let c = COL.none;
      if (h.w && h.r) c = COL.both; else if (h.w) c = COL.wind; else if (h.r) c = COL.rain;
      else if ($('t-cum').checked && cum.has(m.depts[j])) c = COL.cum;
      if (h.any){ nNow++; if(!a.cut) cumNow.add(m.depts[j]); }
      l.setStyle({fillColor:c, fillOpacity: a.cut && h.any ? .45 : .8, color:'#fff', weight:1});
      l.setTooltipContent(`<b>${m.depts[j]}</b><br>Rafales prévues : ${fmt(a.w[j])} km/h<br>` +
        `Pluie prévue, point : ${fmt(a.rp[j])} mm<br>Pluie prévue, moyenne : ${fmt(a.rm[j])} mm` +
        (cum.has(m.depts[j]) ? '<br><i>déjà en orange</i>' : ''));
    });
    // tracks
    trackLayer.clearLayers();
    if ($('t-track').checked && a.track.length){
      const pts = a.track.map(p=>[p[0],p[1]]);
      L.polyline(pts,{color:'#1e2a2b',weight:2}).addTo(trackLayer);
      a.track.forEach(p => L.circleMarker([p[0],p[1]],{radius: p[2]===0?5:3.5, color:'#1e2a2b',
        fillColor: p[2]===0?'#1e2a2b':'#fff', fillOpacity:1, weight:1.5})
        .bindTooltip(p[2]===0 ? 'Position à l’émission' : `+${p[2]} h`).addTo(trackLayer));
    }
    obsvLayer.clearLayers();
    if ($('t-obsv').checked && storm.obsv.length)
      L.polyline(storm.obsv,{color:'#9aa5a6',weight:2,dashArray:'4 4'}).addTo(obsvLayer);
    // pathways panel
    const v = $('voies'); v.hidden = !$('t-voies').checked;
    const nCum = cumNow.size, off = a.cut ? ' off' : '';
    const rainHit = !a.cut && a.rain!=null && a.rain >= m.fcast_rain_mm;
    const expHit = !a.cut && a.exp > 0;
    const orHit = !a.cut && nCum >= m.n_depts;
    v.innerHTML =
      card('Pluie prévue (moy. nationale, 2 j)', `${fmt(a.rain)} mm`, `seuil ${m.fcast_rain_mm} mm · CHIRPS-GEFS du ${a.gefs ?? '—'}`, rainHit, off) +
      card('Exposition prévue à 64 nœuds', `${fmt(a.exp)} pers.`, 'seuil : > 0', expHit, off) +
      card(`Orange DGPC — départements`, `${nNow} à cet avis · ${nCum} cumulés`, `proposition : ≥ ${m.n_depts} départements (avis avant l’heure limite)`, orHit, off) +
      card('Cadre activé à cet avis ?', (rainHit||expHit||orHit) ? 'oui' : 'non',
        a.cut ? 'avis après l’heure limite : aucun déclenchement possible' : 'pluie prévue OU exposition OU orange', (rainHit||expHit||orHit), off);
    $('adv-range').value = idx; $('prev').disabled = idx===0; $('next').disabled = idx===storm.adv.length-1;
  }
  function card(t, val, sub, hit, off){
    return `<div class="v${hit?' hit':''}${off}"><b>${t}</b><div class="val">${val}</div><div style="color:#5e6a6b;font-size:.8rem">${sub}</div></div>`;
  }
  function setStorm(i){
    storm = DATA.storms[i]; idx = 0;
    $('adv-range').max = storm.adv.length-1;
    // start on the first advisory that puts a department in orange, if any
    const first = storm.adv.findIndex(a => DATA.meta.depts.some((d,j)=>hit(a,j).any));
    idx = first >= 0 ? Math.max(0, first-1) : 0;
    render();
  }
  function step(k){ idx = Math.min(Math.max(idx+k,0), storm.adv.length-1); render(); }
  fetch('assets/dgpc-departements.json').then(r=>r.json()).then(data => {
    DATA = data;
    deptLayer = L.geoJSON(data.depts, {style:{color:'#fff',weight:1,fillColor:COL.none,fillOpacity:.8},
      onEachFeature:(f,l)=>l.bindTooltip('', {sticky:true, className:'dep'})}).addTo(map);
    obsvLayer.addTo(map);
    const sel = $('storm-sel');
    data.storms.forEach((s,i)=>{ const o=document.createElement('option'); o.value=i; o.textContent=s.label; sel.appendChild(o); });
    const start = data.storms.findIndex(s=>s.id==='AL142016');
    sel.value = start>=0 ? start : 0;
    sel.onchange = e => setStorm(+e.target.value);
    $('adv-range').oninput = e => { idx = +e.target.value; render(); };
    $('prev').onclick = () => step(-1); $('next').onclick = () => step(1);
    $('play').onclick = () => {
      if (timer){ clearInterval(timer); timer=null; $('play').textContent='▶ Lecture'; return; }
      $('play').textContent = '❚❚ Pause';
      timer = setInterval(()=>{ if(idx>=storm.adv.length-1){ clearInterval(timer); timer=null; $('play').textContent='▶ Lecture'; return; } step(1); }, 700);
    };
    ['t-wind','t-rain','t-cum','t-track','t-obsv','t-voies'].forEach(id => $(id).onchange = render);
    setStorm(+sel.value);
  }).catch(err => { $('adv-lab').textContent = 'Données de la carte indisponibles : ' + err; });
})();
</script>
"""


def _val_cell(v, thr, na=False, decimals=0, missing="n/d"):
    if na or v is None or (np.isscalar(v) and pd.isna(v)):
        return f"<td class='val na'>{missing}</td>"
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


PROPOSED_N = 6
CONFIGS = [
    ("actuel", "Seuils actuels, sans voie rouge", 68, 57, None),
    (
        "orange6",
        f"Proposition : + orange ≥ {PROPOSED_N} dép.",
        68,
        57,
        PROPOSED_N,
    ),
]


def _pathways_section(pdf, deck_trig, n_last_year=14):
    """The framework's pathways and the orange option, storm by storm."""
    if pdf is None or len(pdf) == 0:
        return ""
    cfg_flags = {k: flags(pdf, rf, ro, n) for k, _, rf, ro, n in CONFIGS}

    # Per-pathway return periods under each configuration.
    rp_rows = []
    for k, lab, rf, ro, n in CONFIGS:
        rp = rp_table(cfg_flags[k]).set_index("pathway")
        cells = "".join(
            f"<td class='num'>{int(rp.loc[p, 'n_storms'])}<br>"
            f"<span style='color:#5e6a6b;font-size:.85em'>"
            f"{fr_num(rp.loc[p, 'rp_years'], 1) if np.isfinite(rp.loc[p, 'rp_years']) else 'jamais'}"
            "</span></td>"
            for p in list(PATHWAYS) + ["any"]
        )
        rp_rows.append(
            f"<tr><td class='nm'>{escape(lab)}</td>"
            f"<td class='num'>{rf}</td><td class='num'>{ro}</td>"
            f"<td class='num'>{n if n else '—'}</td>{cells}</tr>"
        )
    heads = "".join(
        f"<th>{escape(v)}</th>" for v in list(PATHWAYS.values()) + ["Ensemble"]
    )

    # The ladder: every department count, thresholds untouched.
    base = cfg_flags["actuel"]["any"]
    ladder = []
    for n in range(1, 11):
        fl = flags(pdf, 68, 57, n)
        act = fl["any"]
        yrs = int(pdf.loc[act, "season"].nunique())
        rp = (dc.SEASON_END - dc.SEASON_START + 2) / yrs if yrs else np.inf
        added = ", ".join(pdf.loc[act & ~base, "label"].astype(str))
        style = (
            " style='background:#fdefe7;font-weight:700'"
            if n == PROPOSED_N
            else ""
        )
        ladder.append(
            f"<tr{style}><td class='num'>{n}"
            + (
                " <span class='pill orange'>proposé</span>"
                if n == PROPOSED_N
                else ""
            )
            + f"</td><td class='num'>{int(act.sum())}</td>"
            f"<td class='num'>{yrs}</td><td class='num'>{fr_num(rp, 1)}</td>"
            f"<td style='white-space:normal'>{escape(added) or '—'}</td></tr>"
        )

    # Storm by storm under the proposed configuration.
    prop = cfg_flags["orange6"]
    cur = cfg_flags["actuel"]
    _, _, rf_p, ro_p, n_p = CONFIGS[1]
    rows = []
    for i, r in pdf.iterrows():
        f = prop.loc[i]
        rows.append(
            "<tr>"
            f"<td class='nm'>{escape(str(r['label']))}</td>"
            + _mark(deck_trig.get(r["atcf_id"], False))
            + _mark(bool(cur.loc[i, "any"]))
            + _val_cell(r["fcast_exp_64"], 1)
            + _val_cell(r["fcast_rain_mm"], rf_p, missing="—")
            + _val_cell(
                r["n_orange"],
                n_p,
                na=not r["orange_rain_known"] and r["n_orange"] < n_p,
            )
            + _val_cell(r["obsv_exp_64"], 1)
            + _val_cell(r["obsv_rain_mm"], ro_p)
            + _mark(bool(f["any"]))
            + "</tr>"
        )
    n_prop = int(prop["any"].sum())
    yrs_prop = int(prop.loc[prop["any"], "season"].nunique())
    rp_prop = (
        (dc.SEASON_END - dc.SEASON_START + 2) / yrs_prop
        if yrs_prop
        else np.inf
    )
    n_cur = int(cur["any"].sum())
    n_deck = int(sum(bool(v) for v in deck_trig.values()))
    added_prop = ", ".join(pdf.loc[prop["any"] & ~base, "label"].astype(str))

    return f"""
<p>
  Le cadre 2026 s’active sur l’une de cinq voies : exposition prévue à des
  vents de 64 nœuds, précipitations prévues, alerte rouge de la DGPC
  confirmée par un <i>Hurricane Warning</i>, exposition observée,
  précipitations observées. La question posée ici : <b>si la voie « rouge »
  est remplacée par une voie « alerte orange dans au moins N départements »,
  quels seuils gardent le même nombre d’activations que le cadre de l’an
  dernier</b> ({n_last_year} tempêtes sur 2002–2025) ?
</p>

<h3>Période de retour de chaque voie</h3>

<p>
  Nombre de tempêtes atteignant chaque voie et période de retour associée
  (saisons avec au moins une tempête), sous trois jeux de seuils. La voie
  rouge n’est pas modélisée ici ; avec elle, le registre du cadre 2026
  compte {n_deck} activations.
</p>

<div class="tablewrap"><table class="wraphead">
<thead><tr><th>Configuration</th><th>Pluie prév.<br>(mm/2 j)</th>
<th>Pluie obs.<br>(mm/2 j)</th><th>Orange<br>(dép. min.)</th>{heads}</tr></thead>
<tbody>
{chr(10).join(rp_rows)}
</tbody></table></div>

<h3>Combien de départements ? Toutes les options, seuils inchangés</h3>

<p>
  Sans voie rouge, les seuils actuels ne retiennent que {n_cur} tempêtes en
  8 saisons. Le tableau donne, pour chaque nombre minimal de départements
  en orange, le registre obtenu en gardant les seuils de pluie et
  d’exposition tels quels.
</p>

<div class="tablewrap"><table class="wraphead">
<thead><tr><th>Orange dans<br>≥ N départements</th><th>Tempêtes</th>
<th>Saisons</th><th>Période de<br>retour (ans)</th>
<th>Tempêtes ajoutées au registre sans voie rouge</th></tr></thead>
<tbody>
{chr(10).join(ladder)}
</tbody></table></div>

<p>
  Aucun seuil ne redonne exactement les {n_last_year} activations du cadre de
  l’an dernier : Dean 2007, Irma 2017 et Elsa 2021 comptent chacune 5
  départements, si bien que le registre passe de 16 tempêtes à 5
  départements à 13 à 6. <b>La proposition retient {n_p} départements</b> :
  {n_prop} tempêtes en {yrs_prop} saisons, période de retour
  {fr_num(rp_prop, 1)} ans, en ajoutant {added_prop}. Ike 2008 est la
  tempête à fort impact que le cadre actuel manque ; Irene et Chantal sont
  le coût de la lecture en rafales. Irma 2017 et Elsa 2021, qui ne
  s’activaient que par la voie rouge, sortent du registre.
</p>

<p>
  Si l’alerte orange de la DGPC était en pratique portée par la pluie
  seule, ou par le vent soutenu plutôt que les rafales, 3 départements
  donneraient exactement 14 tempêtes (Dean, Ike, Irene, Elsa) et Chantal
  ne compterait jamais. Cette lecture dépend de la pratique de la DGPC et
  n’est pas retenue ici.
</p>

<h3>Tempête par tempête, sous la proposition (orange ≥ {n_p} départements)</h3>

<p>
  Valeurs de chaque voie ; cases orange = seuil atteint sous la
  proposition (pluie prévue ≥ {rf_p} mm, pluie observée ≥ {ro_p} mm, alerte
  orange dans ≥ {n_p} départements, exposition &gt; 0). Exposition en
  personnes ; pluie en mm sur 2 jours (moyenne nationale) ; orange en
  nombre de départements.
</p>

<div class="tablewrap"><table class="wraphead">
<thead><tr>
  <th>Tempête</th><th>Cadre 2026<br>(registre)</th><th>Seuils actuels<br>sans rouge</th>
  <th>Exposition<br>prévue</th><th>Pluie<br>prévue</th><th>Orange<br>(dép.)</th>
  <th>Exposition<br>observée</th><th>Pluie<br>observée</th><th>Proposition</th>
</tr></thead>
<tbody>
{chr(10).join(rows)}
</tbody></table></div>

<p class="legend">
  Les indicateurs du cadre (heure limite, pluie prévue à l’échéance
  Action, pluie observée) sont repris du rejeu historique utilisé pour
  calibrer les seuils 2026, de sorte que la colonne « seuils actuels »
  reproduit le registre des diapositives hors voie rouge. « — » en pluie
  prévue : aucun avis émis avant l’heure limite ne prévoyait de passage à
  moins de {D_THRESH} km. Laura et Isaias 2020 n’ont pas de prévision
  CHIRPS-GEFS : leur colonne orange ne compte que le vent.
</p>
"""


def render(tbl, verdicts, counts, freq, chart="", notes=None, pathways=None):
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
<style>{CSS}{EXTRA_CSS}{MAP_CSS}</style>
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

<h2>3. Les voies de déclenchement du cadre et l’option « orange »</h2>

{_pathways_section(*(pathways or (None, {})))}

<h2>4. Combien de tempêtes placent au moins un département en orange</h2>

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

<h2>5. Le détail par département</h2>

<p>
  Les trois matrices ci-dessous donnent, pour chaque tempête et chaque
  département, la valeur maximale prévue pendant l’approche. Les cases
  orange atteignent le seuil. Départements du sud-ouest au nord-est.
</p>

<h3>Rafales prévues maximales dans le département (km/h)</h3>
{_matrix(verdicts, storms_order, "wind_max_kmh", dc.ORANGE_WIND_KMH[0])}

<h3>Pluie prévue — moyenne du département (mm / jour)</h3>
{_matrix(verdicts, storms_order, "rain_mean_max_mm", thr_mm)}

<h3>Pluie prévue — point le plus arrosé du département (mm / jour)</h3>
{_matrix(verdicts, storms_order, "rain_pix_max_mm", thr_mm)}

<h2>6. Préavis</h2>

<p>
  Pour les tempêtes ayant placé au moins un département en orange : heures
  entre la première prévision atteignant le critère (dans n’importe quel
  département) et le passage au plus près. Une valeur négative (en rouge)
  signifie que le seuil n’a été prévu qu’après le passage — l’alerte
  aurait été tardive.
</p>

{_lead_table(counts)}

<h2>7. Fréquence par département</h2>

<p>
  Nombre de tempêtes ayant placé chaque département en orange sur
  {dc.SEASON_START}–{dc.SEASON_END}, et période de retour correspondante.
</p>

{_freq_table(freq)}

<h2>8. Avis par avis : quand l’alerte orange serait tombée</h2>

<p>
  Choisir une tempête, puis faire défiler les avis du NHC. Pour chaque
  avis, la carte montre les départements que la prévision associée aurait
  placés en alerte orange (rafales, pluie au point le plus arrosé, ou les
  deux), ceux déjà en orange sur un avis précédent émis avant l’heure
  limite, la trajectoire prévue, et l’état des voies du cadre à cet
  instant. Survoler un département pour lire les valeurs. Chaque couche se
  masque d’un clic.
</p>

{MAP_HTML}

<p class="legend">
  Violet : département en orange par le vent <i>et</i> la pluie. Les avis
  émis après l’heure limite sont affichés en transparence : ils ne peuvent
  plus déclencher le cadre ni entrer dans le compte des départements.
  Fond de carte OpenStreetMap ; polygones départementaux simplifiés.
</p>

<h2>9. Méthode et limites</h2>

<ul>
  <li><b>Tempêtes</b> : centre analysé passé à moins de {D_THRESH} km
      d’Haïti, {dc.SEASON_START}–{dc.SEASON_END} — le jeu de la première
      analyse. Le registre du cadre retient aussi des tempêtes plus
      lointaines ayant causé des impacts (Ivan 2004), marquées n/d.</li>
  <li><b>Avis considérés</b> : chaque avis du NHC dont le passage au plus
      près prévu est à {LT_CUTOFF_HRS} h ou plus — la même heure limite que
      les voies de prévision du cadre. Les indicateurs de cutoff et de
      pluie prévue sont repris du rejeu historique qui a servi à calibrer
      les seuils 2026 (Melissa 2025 : valeurs du registre).</li>
  <li><b>Prévisions de pluie</b> : pour chaque avis, la prévision
      CHIRPS-GEFS du jour (ou la plus récente), sur les jours où la
      trajectoire prévue passe à moins de {D_THRESH} km d’Haïti — la règle
      d’attribution du cadre. Produit <b>journalier</b> : la valeur d’un
      jour calendaire tient lieu du cumul sur 24 h, ce qui sous-estime
      légèrement un cumul glissant. Moyennes départementales sur un
      ré-échantillonnage à {dc.GEFS_UPSAMPLE_RES}°.</li>
  <li><b>Prévisions de vent</b> : champ de vent paramétrique reconstruit à
      partir des rayons de vent de chaque avis du NHC, vent soutenu réduit
      sur terre (×0,85) puis converti en rafales (×1,25, facteur
      conventionnel en terrain dégagé), maximum dans chaque département.
      Méthode et validation dans l’analyse archivée.</li>
  <li><b>Un avis suffit</b> : un département est compté en orange dès
      qu’un avis émis avant l’heure limite atteint le critère, quelle que
      soit l’échéance. La lecture en rafales est nettement plus permissive
      que celle en vent soutenu de l’archive : Chantal 2013, simple tempête
      tropicale passée au large, place ainsi les dix départements en orange
      sur ses prévisions à cinq jours.</li>
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
{MAP_JS}
</body>
</html>
"""
