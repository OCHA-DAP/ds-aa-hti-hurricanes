"""Render the standalone advisory-by-advisory map (``docs/carte-avis.html``).

Pick a storm, step through its NHC advisories, and see for each one what
the forecasts of the moment implied: the wind buffers at 34 / 50 / 64 kt,
the observed swath so far, the departments DGPC would have placed in
orange on the paired CHIRPS-GEFS forecast and the gust field, and the
framework's own pathway values. Every layer toggles.

Data: ``docs/assets/carte/index.json`` plus one file per storm, written
by ``src.dgpc.map_data``.
"""

from datetime import date
from html import escape

import numpy as np
import pandas as pd

from src.constants import D_THRESH, LT_CUTOFF_HRS
from src.dgpc import constants as dc
from src.dgpc.dept_page import PROPOSED_N
from src.dgpc.page import CSS, MONTHS_FR, fr_num

MAP_CSS = """
.wrap.wide{max-width:74rem}
#carte-wrap{background:var(--card);border:1px solid var(--line);border-radius:10px;
padding:1rem;margin:1.4rem 0}
#carte{height:min(72vh,640px);border-radius:8px;border:1px solid var(--line)}
.ctl{display:flex;flex-wrap:wrap;gap:.6rem 1.2rem;align-items:center;
margin-bottom:.6rem;font-size:.92rem}
.ctl select,.ctl button{font:inherit;padding:.3rem .55rem;border:1px solid var(--line);
border-radius:6px;background:#fff}
.ctl button{cursor:pointer}
.ctl button:disabled{opacity:.4;cursor:default}
.ctl input[type=range]{width:min(420px,100%)}
.ctl label{white-space:nowrap}
.advlab{font-weight:700}
.badge{display:inline-block;font-size:.74rem;font-weight:700;padding:.12rem .5rem;
border-radius:5px;margin-left:.4rem;vertical-align:middle}
.badge.ok{background:#e7f4ee;color:#0f8a5f}
.badge.cut{background:#fbeaea;color:#a02a2a}
.toggles{display:flex;flex-wrap:wrap;gap:.3rem 1.1rem;font-size:.88rem;margin:.5rem 0}
.toggles .grp{font-size:.74rem;text-transform:uppercase;letter-spacing:.06em;
color:var(--muted);font-weight:700;margin-right:-.4rem}
.toggles label{display:inline-flex;align-items:center;gap:.3rem;cursor:pointer}
.sw{display:inline-block;width:.9rem;height:.9rem;border-radius:3px;border:1px solid #0002;
vertical-align:-2px}
.sw.line{height:0;border:0;border-top:3px solid}
#voies{display:grid;grid-template-columns:repeat(auto-fit,minmax(13rem,1fr));gap:.6rem;
margin-top:.8rem;font-size:.88rem}
#voies .v{border:1px solid var(--line);border-radius:8px;padding:.55rem .75rem;background:#fafcfc}
#voies .v.hit{border-color:#0f8a5f;background:#e7f4ee}
#voies .v.off{opacity:.55}
#voies .v b{display:block;font-size:.8rem;color:var(--muted);text-transform:uppercase;
letter-spacing:.04em;margin-bottom:.15rem}
#voies .v .val{font-size:1.15rem;font-weight:700;font-variant-numeric:tabular-nums}
.leaflet-tooltip.dep{font-size:.82rem;line-height:1.35}
#loading{font-size:.86rem;color:var(--muted)}
td.val{text-align:right;font-variant-numeric:tabular-nums;color:#5e6a6b}
td.val.hit{background:#fdefe7;color:#a4551f;font-weight:700}
td.val.na{color:#9aa5a6;text-align:center}
table.act{font-size:.8rem}
table.act th{text-align:center;vertical-align:middle;font-size:.68rem;padding:.35rem .35rem;
letter-spacing:.02em}
table.act td{padding:.32rem .4rem}
table.act td.nm{white-space:nowrap;font-size:.8rem}
table.act td.bar{background-repeat:no-repeat;background-size:100% 100%}
table.act td.cerf{white-space:nowrap;font-size:.78rem}
table.act td.cerf.yes{background:#c62828;color:#fff;font-weight:700}
table.act .also{font-size:.68rem;color:#9aa5a6;font-weight:400}
table.act .also.over{color:#a4551f}
table.act th.g1{background:#e3ecf8}
table.act th.g2{background:#fdefe7}
table.act th.g3{background:#eef2f5}
table.act th .thr{font-weight:400;text-transform:none;letter-spacing:0;font-size:.74rem;color:#5e6a6b}
table.act th.g4{background:#e7f4ee}
table.act td.nm a{color:var(--blue);text-decoration:none}
table.act td.nm a:hover{text-decoration:underline}
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
    <button id="fit" type="button" title="Cadrer sur Haïti">⌖ Haïti</button>
    <span id="adv-lab" class="advlab"></span>
    <span id="loading"></span>
  </div>
  <div class="toggles">
    <span class="grp">Alerte orange DGPC (simulée)</span>
    <label><input type="checkbox" id="t-wind" checked>
      <svg class="sw" viewBox="0 0 14 14"><rect width="14" height="14" fill="url(#pat-wind)"/></svg> rafales ≥ 100 km/h (hachures)</label>
    <label><input type="checkbox" id="t-rain" checked>
      <svg class="sw" viewBox="0 0 14 14"><rect width="14" height="14" fill="url(#pat-rain)"/></svg> pluie ≥ 100 mm au point (points)</label>
    <label><span class="sw" style="background:#ec835a"></span> les deux (plein)</label>
    <label><input type="checkbox" id="t-cum" checked>
      <span class="sw" style="background:#f7d9c9"></span> déjà en orange (avis précédents, avant l’heure limite)</label>
  </div>
  <div class="toggles">
    <span class="grp">Vent prévu (NHC)</span>
    <label><input type="checkbox" id="t-b34">
      <span class="sw" style="background:#fff3b0;border-color:#c9a800"></span> 34 nœuds</label>
    <label><input type="checkbox" id="t-b50">
      <span class="sw" style="background:#ffd28a;border-color:#d17a00"></span> 50 nœuds</label>
    <label><input type="checkbox" id="t-b64" checked>
      <span class="sw" style="background:#f4a0a0;border-color:#c0392b"></span> 64 nœuds (déclencheur d’exposition)</label>
    <label><input type="checkbox" id="t-swath">
      <span class="sw" style="background:#c9b3e6;border-color:#6c3fb0"></span> Bande observée à 64 nœuds (cumulée à l’émission)</label>
  </div>
  <div class="toggles">
    <span class="grp">Trajectoires</span>
    <label><input type="checkbox" id="t-track" checked>
      <span class="sw line" style="border-color:#1e2a2b"></span> prévue</label>
    <label><input type="checkbox" id="t-obsv">
      <span class="sw line" style="border-color:#9aa5a6;border-top-style:dashed"></span> observée</label>
    <label><input type="checkbox" id="t-voies" checked> Voies du cadre</label>
  </div>
  <svg width="0" height="0" style="position:absolute" aria-hidden="true">
    <defs>
      <pattern id="pat-wind" patternUnits="userSpaceOnUse" width="7" height="7" patternTransform="rotate(45)">
        <rect width="7" height="7" fill="#fff"/><rect width="3.5" height="7" fill="#ec835a"/>
      </pattern>
      <pattern id="pat-rain" patternUnits="userSpaceOnUse" width="7" height="7">
        <rect width="7" height="7" fill="#fff"/><circle cx="3.5" cy="3.5" r="2" fill="#ec835a"/>
      </pattern>
    </defs>
  </svg>
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
  let META, DEPTS, INDEX, storm, idx = 0, timer = null;
  const cache = {};
  const map = L.map('carte', {scrollWheelZoom:false}).setView([18.9,-72.8], 7);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',
    {attribution:'&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>', maxZoom:12, opacity:.55}).addTo(map);
  const bufLayer = L.layerGroup().addTo(map), swathLayer = L.layerGroup().addTo(map);
  let deptLayer; const trackLayer = L.layerGroup(), obsvLayer = L.layerGroup();
  const COL = {none:'#e9eef0', cum:'#f7d9c9', wind:'url(#pat-wind)', rain:'url(#pat-rain)', both:'#ec835a'};
  const BUF = {34:{c:'#c9a800',f:'#fff3b0'}, 50:{c:'#d17a00',f:'#ffd28a'}, 64:{c:'#c0392b',f:'#f4a0a0'}};

  function hit(a, j){
    const w = $('t-wind').checked && a.w[j]!=null && a.w[j] >= META.wind_kmh;
    const r = $('t-rain').checked && a.rp[j]!=null && a.rp[j] >= META.rain_mm;
    return {w, r, any: w||r};
  }
  function cumBefore(i){
    const s = new Set();
    for (let k=0;k<i;k++){ const a=storm.adv[k]; if(a.cut) continue;
      META.depts.forEach((d,j)=>{ if(hit(a,j).any) s.add(d); }); }
    return s;
  }
  function render(){
    if (!storm) return;
    const a = storm.adv[idx], m = META, cum = cumBefore(idx);
    const d = new Date(a.t+'Z');
    $('adv-lab').innerHTML = `Avis du ${d.toLocaleString('fr-FR',{timeZone:'UTC',day:'numeric',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'})} UTC` +
      ` · passage au plus près prévu dans ${fmt(a.ttc)} h` +
      (a.cut ? '<span class="badge cut">après l’heure limite</span>' : '<span class="badge ok">avant l’heure limite</span>') +
      ` <span style="color:#5e6a6b;font-weight:400">(${idx+1}/${storm.adv.length})</span>`;
    let nNow = 0; const cumNow = new Set(cum);
    deptLayer.eachLayer(l => {
      const j = m.depts.indexOf(l.feature.properties.name); const h = hit(a,j);
      let c = COL.none;
      if (h.w && h.r) c = COL.both; else if (h.w) c = COL.wind; else if (h.r) c = COL.rain;
      else if ($('t-cum').checked && cum.has(m.depts[j])) c = COL.cum;
      if (h.any){ nNow++; if(!a.cut) cumNow.add(m.depts[j]); }
      l.setStyle({fillColor:c, fillOpacity: a.cut && h.any ? .55 : .95, color:'#fff', weight:1});
      l.setTooltipContent(`<b>${m.depts[j]}</b><br>Rafales prévues : ${fmt(a.w[j])} km/h<br>` +
        `Pluie prévue, point : ${fmt(a.rp[j])} mm<br>Pluie prévue, moyenne : ${fmt(a.rm[j])} mm` +
        (cum.has(m.depts[j]) ? '<br><i>déjà en orange</i>' : ''));
    });
    // wind buffers, largest first so the 64 kt core sits on top
    bufLayer.clearLayers();
    [34,50,64].forEach(s => {
      if (!$('t-b'+s).checked || !a.buf[s]) return;
      L.geoJSON(a.buf[s], {style:{color:BUF[s].c, weight:1.2, fillColor:BUF[s].f, fillOpacity:.35, dashArray: a.cut?'3 3':null}})
        .bindTooltip(`Vents prévus ≥ ${s} nœuds`, {sticky:true}).addTo(bufLayer);
    });
    swathLayer.clearLayers();
    if ($('t-swath').checked && a.swath)
      L.geoJSON(a.swath, {style:{color:'#6c3fb0', weight:1, fillColor:'#c9b3e6', fillOpacity:.45}})
        .bindTooltip('Bande observée ≥ 64 nœuds, cumulée à l’émission', {sticky:true}).addTo(swathLayer);
    trackLayer.clearLayers();
    if ($('t-track').checked && a.track.length){
      L.polyline(a.track.map(p=>[p[0],p[1]]),{color:'#1e2a2b',weight:2}).addTo(trackLayer);
      a.track.forEach(p => L.circleMarker([p[0],p[1]],{radius: p[2]===0?5:3.5, color:'#1e2a2b',
        fillColor: p[2]===0?'#1e2a2b':'#fff', fillOpacity:1, weight:1.5})
        .bindTooltip((p[2]===0 ? 'Position à l’émission' : `+${p[2]} h`) + (p[3]!=null ? ` · ${fmt(p[3])} nœuds` : '')).addTo(trackLayer));
    }
    obsvLayer.clearLayers();
    if ($('t-obsv').checked && storm.obsv.length)
      L.polyline(storm.obsv,{color:'#9aa5a6',weight:2,dashArray:'4 4'}).addTo(obsvLayer);
    const v = $('voies'); v.hidden = !$('t-voies').checked;
    const nCum = cumNow.size, off = a.cut ? ' off' : '';
    const rainHit = !a.cut && a.rain!=null && a.rain >= m.fcast_rain_mm;
    const expHit = !a.cut && a.exp > 0;
    const orHit = !a.cut && nCum >= m.n_depts;
    v.innerHTML =
      card('Pluie prévue (moy. nationale, 2 j)', `${fmt(a.rain)} mm`, `seuil ${m.fcast_rain_mm} mm · CHIRPS-GEFS du ${a.gefs ?? '—'}`, rainHit, off) +
      card(`Exposition prévue à ${m.exposure_kt} nœuds`, `${fmt(a.exp)} pers.`, 'seuil : > 0 · zone prévue hors bande déjà observée', expHit, off) +
      card('Orange DGPC simulée — départements', `${nNow} à cet avis · ${nCum} cumulés`, `estimation d’après les seuils de la DGPC, pas son registre · proposition : ≥ ${m.n_depts} départements`, orHit, off) +
      card('Cadre activé à cet avis ?', (rainHit||expHit||orHit) ? 'oui' : 'non',
        a.cut ? 'avis après l’heure limite : aucun déclenchement possible' : 'pluie prévue OU exposition OU orange', (rainHit||expHit||orHit), off);
    $('adv-range').value = idx; $('prev').disabled = idx===0; $('next').disabled = idx===storm.adv.length-1;
  }
  function card(t, val, sub, hit, off){
    return `<div class="v${hit?' hit':''}${off}"><b>${t}</b><div class="val">${val}</div><div style="color:#5e6a6b;font-size:.8rem">${sub}</div></div>`;
  }
  function loadStorm(i){
    const meta = INDEX[i];
    if (timer){ clearInterval(timer); timer=null; $('play').textContent='▶ Lecture'; }
    if (cache[meta.id]) return Promise.resolve(cache[meta.id]);
    $('loading').textContent = 'chargement…';
    return fetch(`assets/carte/${meta.id}.json`).then(r=>r.json()).then(d => { cache[meta.id]=d; $('loading').textContent=''; return d; });
  }
  function setStorm(i){
    loadStorm(i).then(d => {
      storm = d; $('adv-range').max = storm.adv.length-1;
      const first = storm.adv.findIndex(a => META.depts.some((d,j)=>hit(a,j).any));
      idx = first >= 0 ? Math.max(0, first-1) : 0;
      history.replaceState(null, '', '#'+storm.id);
      render();
    });
  }
  function step(k){ idx = Math.min(Math.max(idx+k,0), storm.adv.length-1); render(); }
  fetch('assets/carte/index.json').then(r=>r.json()).then(data => {
    META = data.meta; DEPTS = data.depts; INDEX = data.storms;
    deptLayer = L.geoJSON(DEPTS, {style:{color:'#fff',weight:1,fillColor:COL.none,fillOpacity:.95},
      onEachFeature:(f,l)=>l.bindTooltip('', {sticky:true, className:'dep'})}).addTo(map);
    trackLayer.addTo(map); obsvLayer.addTo(map);
    const sel = $('storm-sel');
    INDEX.forEach((s,i)=>{ const o=document.createElement('option'); o.value=i; o.textContent=s.label; sel.appendChild(o); });
    const want = location.hash.replace('#','') || 'AL142016';
    const start = INDEX.findIndex(s=>s.id===want);
    sel.value = start>=0 ? start : 0;
    sel.onchange = e => setStorm(+e.target.value);
    $('adv-range').oninput = e => { idx = +e.target.value; render(); };
    $('prev').onclick = () => step(-1); $('next').onclick = () => step(1);
    $('fit').onclick = () => map.setView([18.9,-72.8], 7);
    $('play').onclick = () => {
      if (timer){ clearInterval(timer); timer=null; $('play').textContent='▶ Lecture'; return; }
      $('play').textContent = '❚❚ Pause';
      timer = setInterval(()=>{ if(idx>=storm.adv.length-1){ clearInterval(timer); timer=null; $('play').textContent='▶ Lecture'; return; } step(1); }, 700);
    };
    document.querySelectorAll('td.nm a[data-storm]').forEach(el => el.onclick = ev => {
      ev.preventDefault(); const k = INDEX.findIndex(s=>s.id===el.dataset.storm);
      if (k>=0){ sel.value=k; setStorm(k); $('carte-wrap').scrollIntoView({behavior:'smooth'}); }
    });
    document.addEventListener('keydown', e => {
      if (e.target.tagName==='SELECT' || e.target.tagName==='INPUT') return;
      if (e.key==='ArrowLeft') step(-1); else if (e.key==='ArrowRight') step(1);
    });
    ['t-wind','t-rain','t-cum','t-b34','t-b50','t-b64','t-swath','t-track','t-obsv','t-voies'].forEach(id => $(id).onchange = render);
    setStorm(+sel.value);
  }).catch(err => { $('adv-lab').textContent = 'Données de la carte indisponibles : ' + err; });
})();
</script>
"""


def _num(v, thr, na=False, decimals=0, missing="—", also=None):
    """A value cell, shaded when the threshold is met.

    ``also`` is the same indicator with the cutoff ignored; it is shown in
    small brackets when it differs from the pre-cutoff value.
    """
    extra = ""
    if also is not None and not (np.isscalar(also) and pd.isna(also)):
        same = (
            v is not None
            and not (np.isscalar(v) and pd.isna(v))
            and abs(float(also) - float(v)) < 0.5
        )
        if not same:
            over = " over" if also >= thr else ""
            extra = (
                f" <span class='also{over}'>[{fr_num(also, decimals)}]</span>"
            )
    if na or v is None or (np.isscalar(v) and pd.isna(v)):
        return f"<td class='val na'>{missing}{extra}</td>"
    cls = "val hit" if v >= thr else "val"
    return f"<td class='{cls}'>{fr_num(v, decimals)}{extra}</td>"


def _yes(flag):
    if flag is None or (np.isscalar(flag) and pd.isna(flag)):
        return "<td class='val na'>n/d</td>"
    return (
        "<td class='val hit' style='text-align:center'>✓</td>"
        if flag
        else "<td class='val' style='text-align:center'>—</td>"
    )


def _activation_table(pdf, meta):
    """Every storm: what would have activated, and why.

    Columns are grouped: the framework's own thresholds (forecast and
    observed, each wind exposure and rain), then the simulated DGPC
    orange count, then the record - impact and CERF.
    """
    from src.dgpc.pathways import flags

    rf, ro, n_p = (
        meta["fcast_rain_mm"],
        meta["obsv_rain_mm"],
        meta["n_depts"],
    )
    hard = flags(pdf, rf, ro, None)
    full = flags(pdf, rf, ro, n_p)
    pop_col = pdf["pop_affected_n"]
    pop_max = float(pop_col.max()) if pop_col.notna().any() else 1.0
    order = pdf.sort_values(
        ["pop_affected_n", "season"],
        ascending=[False, True],
        na_position="last",
    ).index
    rows = []
    for i in order:
        r = pdf.loc[i]
        f = full.loc[i]
        cerf = r.get("cerf")
        cerf = "" if cerf is None or pd.isna(cerf) else str(cerf)
        cerf_yes = cerf.startswith("$") or cerf == "combined"
        if cerf.startswith("$"):
            try:
                cerf = (
                    f"{float(cerf[1:].replace(',', '')) / 1e6:.1f} M$".replace(
                        ".", ","
                    )
                )
            except ValueError:
                pass
        pop = r.get("pop_affected_n")
        if pop is not None and pd.notna(pop) and pop > 0:
            # Square-root scale: Matthew and Melissa are 2 M, most storms
            # under 50 k; a linear bar would flatten everything else.
            w = max(1.5, 100 * (float(pop) / pop_max) ** 0.5)
            pop_cell = (
                "<td class='val bar' style='background-image:linear-gradient("
                f"to right,#cfe0f7 {w:.1f}%,transparent {w:.1f}%)'>"
                f"{fr_num(pop)}</td>"
            )
        else:
            pop_cell = "<td class='val'>—</td>"
        cerf_cell = (
            f"<td class='cerf{' yes' if cerf_yes else ''}'>"
            f"{escape(cerf) or '—'}</td>"
        )
        orange_na = (not r["orange_rain_known"]) and r["n_orange"] < n_p
        rows.append(
            "<tr>"
            f"<td class='nm'><a href='#{r['atcf_id']}' data-storm='{r['atcf_id']}'>"
            f"{escape(str(r['label']))}</a></td>"
            + _num(r["fcast_exp_64"], 1, also=r.get("fcast_exp_64_all"))
            + _num(r["fcast_rain_mm"], rf, also=r.get("fcast_rain_mm_all"))
            + _num(r["obsv_exp_64"], 1)
            + _num(r["obsv_rain_mm"], ro)
            + _num(
                r["n_orange"], n_p, na=orange_na, also=r.get("n_orange_all")
            )
            + _yes(bool(f["any"]))
            + pop_cell
            + cerf_cell
            + "</tr>"
        )
    tot = {
        "full": int(full["any"].sum()),
        "fe": int(hard["fcast_exp"].sum()),
        "fr": int(hard["fcast_rain"].fillna(False).sum()),
        "oe": int(hard["obsv_exp"].sum()),
        "or": int(hard["obsv_rain"].fillna(False).sum()),
        "on": int(full["orange"].sum()),
    }
    return f"""
<div class="tablewrap"><table class="wraphead act">
<thead>
<tr>
  <th rowspan="3">Tempête</th>
  <th colspan="4" class="g1">Indicateurs mesurés</th>
  <th rowspan="3" class="g2">Alerte orange<br>DGPC (simulée)<br><span class="thr">dép. ≥ {n_p}</span></th>
  <th rowspan="3" class="g4">Activation</th>
  <th colspan="2" class="g3">Historique</th>
</tr>
<tr>
  <th colspan="2" class="g1">Prévision</th>
  <th colspan="2" class="g1">Observation</th>
  <th rowspan="2" class="g3">Pop.<br>affectée</th>
  <th rowspan="2" class="g3">CERF</th>
</tr>
<tr>
  <th class="g1">Expo. 64 nds<br><span class="thr">&gt; 0</span></th>
  <th class="g1">Pluie 2 j (mm)<br><span class="thr">≥ {rf}</span></th>
  <th class="g1">Expo. 64 nds<br><span class="thr">&gt; 0</span></th>
  <th class="g1">Pluie 2 j (mm)<br><span class="thr">≥ {ro}</span></th>
</tr>
</thead>
<tbody>
{chr(10).join(rows)}
</tbody>
<tfoot><tr style="background:#f7fafb;font-weight:700">
  <td>Total ({len(pdf)})</td>
  <td class='val'>{tot["fe"]}</td><td class='val'>{tot["fr"]}</td>
  <td class='val'>{tot["oe"]}</td><td class='val'>{tot["or"]}</td>
  <td class='val'>{tot["on"]}</td>
  <td class='val' style='text-align:center'>{tot["full"]}</td>
  <td></td><td></td>
</tr></tfoot>
</table></div>
"""


def render(n_storms, pdf=None, meta=None, deck_trig=None):
    today = date.today()
    stamp = f"{MONTHS_FR[today.month]} {today.year}"
    table_section = ""
    if pdf is not None and len(pdf):
        table_section = f"""
<h2>Tempête par tempête : ce qui aurait déclenché, et pourquoi</h2>

<p style="max-width:none">
  Chaque tempête du jeu, par population affectée décroissante, avec la
  valeur de chaque indicateur sur les avis émis avant l’heure limite.
  Cases orange : seuil atteint. Les <b>indicateurs mesurés</b> sont les
  seuils purement quantitatifs — prévision ou observation, exposition ou
  précipitations. La colonne <b>activation</b> est le verdict d’ensemble
  de la proposition : l’un des indicateurs mesurés atteint, <i>ou</i>
  l’alerte orange simulée dans au moins {meta["n_depts"]} départements.
  Cliquer sur une tempête pour la charger dans la carte.
</p>

{_activation_table(pdf, meta)}

<p class="legend" style="max-width:none">
  Entre crochets, en petit : la même valeur en ignorant l’heure limite —
  ce que les prévisions ont fini par dire, même trop tard pour agir ;
  en orange lorsque ce maximum tardif atteint le seuil. Sans crochets,
  les deux valeurs sont identiques.
  Exposition en personnes ; précipitations en mm sur deux jours glissants
  (moyenne nationale, CHIRPS-GEFS en prévision, IMERG en observation) ;
  alerte orange en nombre de départements (sur 10). « — » en prévision :
  aucun avis avant l’heure limite ne prévoyait de passage à moins de
  {D_THRESH} km. Laura et Isaias 2020 n’ont pas de prévision CHIRPS-GEFS
  archivée : leur compte de départements ne tient qu’au vent. Population
  affectée : EM-DAT, reprise des diapositives. CERF : « pre- » = antérieur
  à la création du CERF (2006) ; « combined » = allocation combinée
  Fay/Gustav/Hanna/Ike (2008).
</p>
"""
    return f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Avis par avis — ouragans, Haïti</title>
<style>{CSS}{MAP_CSS}</style>
</head>
<body>
<div class="wrap wide">

<header>
  <p class="crumb"><a href="./">←
  Action anticipatoire · Ouragans · Haïti</a></p>
  <div class="kicker">Outil · {escape(stamp)}</div>
  <h1>Avis par avis : ce que les prévisions disaient, et ce que le cadre
  et la DGPC en auraient fait</h1>
  <p class="sub">
    Choisir une tempête, puis faire défiler les avis du NHC. Pour chaque
    avis : la trajectoire et les rayons de vent prévus, la bande de vents
    déjà observée, les départements que la prévision de pluie et le champ
    de rafales auraient placés en alerte orange, et l’état des voies de
    déclenchement du cadre à cet instant. {n_storms} tempêtes passées à
    moins de {D_THRESH} km d’Haïti, {dc.SEASON_START}–{dc.SEASON_END}.
  </p>
</header>

<div class="callout">
  <h3>Les alertes orange affichées sont une simulation, pas un registre</h3>
  <p>
    La DGPC n’a pas fourni l’historique de ses alertes. Les départements
    « en orange » sur cette carte et dans le tableau sont une
    <b>estimation</b> : les seuils que la DGPC a indiqués (100 mm en 24 h,
    rafales de 100 km/h), appliqués par nos soins aux prévisions de
    l’époque. Ils montrent ce que ces seuils auraient donné, non ce que la
    DGPC a réellement déclaré.
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
  affichés en transparence. Flèches ← → pour changer d’avis. Le lien de
  la page retient la tempête choisie.
</p>

<div class="note" style="max-width:none">
  <h3>Ce qui est calculé, et comment</h3>
  <ul>
    <li><b>Pluie prévue</b> : prévision CHIRPS-GEFS du jour de l’avis (ou la
        plus récente), moyenne nationale sur deux jours glissants pour la
        voie du cadre (seuil 68 mm), et point le plus arrosé de chaque
        département pour l’alerte orange (seuil 100 mm / 24 h), sur les
        jours où la trajectoire prévue passe à moins de {D_THRESH} km.</li>
    <li><b>Rafales prévues</b> : champ de vent paramétrique reconstruit à
        partir des rayons de vent de l’avis, vent soutenu réduit sur terre
        (×0,85) puis converti en rafales (×1,25), maximum dans chaque
        département (seuil orange 100 km/h).</li>
    <li><b>Exposition prévue</b> : population dans le polygone de vents de
        64 nœuds prévu, hors bande déjà observée, plus la population de
        cette bande — la convention du déclencheur.</li>
    <li><b>Alerte orange DGPC</b> : un département est compté dès qu’un
        avis émis avant l’heure limite atteint l’un des deux critères ; la
        proposition retient <b>{PROPOSED_N} départements</b>. Voir
        <a href="dgpc-departements.html">l’analyse par département</a>.</li>
  </ul>
</div>

{table_section}

<footer>
  OCHA · Centre de données humanitaires — outil préparé pour la révision
  2026 du cadre d’action anticipatoire pour les tempêtes et ouragans en
  Haïti. Code et méthode :
  <a href="https://github.com/OCHA-DAP/ds-aa-hti-hurricanes"
  >ds-aa-hti-hurricanes</a>.
  Données : NHC (avis de prévision et rayons de vent), CHIRPS-GEFS (UCSB
  CHC), WorldPop 2026. Fond de carte OpenStreetMap. Mise à jour
  {escape(stamp)}.
</footer>

</div>
{MAP_JS}
</body>
</html>
"""
