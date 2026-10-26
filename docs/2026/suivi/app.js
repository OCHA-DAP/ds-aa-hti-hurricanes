// Monitoring page: reads the static files under data/ (written by
// src/monitoring/site_export.py, published by .github/workflows/deploy-pages.yml).
(function () {
  'use strict';
  const $ = (id) => document.getElementById(id);
  const DATA = 'data/';
  const STAGES = ['mobilisation', 'action'];
  const cache = {};
  let INDEX, GEO, storm = null, advIdx = 0, map, layers = {}, stormReq = 0, advReq = 0;

  // ---------- formatting ----------
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g,
    (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const num = (v, d = 0) => (v == null ? '—'
    : Number(v).toLocaleString('fr-FR', { maximumFractionDigits: d, minimumFractionDigits: d }));
  const pers = (v) => (v == null ? '—' : num(v, 0));
  const mm = (v) => (v == null ? '—' : `${num(v, 1)} mm`);
  const dt = (iso) => (iso ? new Date(iso) : null);
  const fmtTime = (iso) => {
    const d = dt(iso); if (!d) return '—';
    return d.toLocaleString('fr-FR', { timeZone: 'UTC', day: 'numeric', month: 'short',
      year: 'numeric', hour: '2-digit', minute: '2-digit' }) + ' UTC';
  };
  const fmtTimeShort = (iso) => {
    const d = dt(iso); if (!d) return '—';
    return d.toLocaleString('fr-FR', { timeZone: 'UTC', day: 'numeric', month: 'short',
      hour: '2-digit', minute: '2-digit' });
  };
  const fmtDate = (s) => {
    if (!s) return '—';
    return new Date(s + 'T00:00:00Z').toLocaleDateString('fr-FR',
      { timeZone: 'UTC', day: 'numeric', month: 'short', year: 'numeric' });
  };
  const fmtDay = (s) => new Date(s + 'T00:00:00Z').toLocaleDateString('fr-FR',
    { timeZone: 'UTC', weekday: 'short', day: 'numeric', month: 'short' });
  const ago = (iso) => {
    const h = (Date.now() - dt(iso).getTime()) / 3.6e6;
    if (h < 1) return 'il y a moins d’une heure';
    if (h < 48) return `il y a ${Math.round(h)} h`;
    return `il y a ${Math.round(h / 24)} jours`;
  };
  const daysOld = (dateStr) => (Date.now() - new Date(dateStr + 'T00:00:00Z').getTime()) / 864e5;

  async function getJSON(path) {
    if (!cache[path]) {
      cache[path] = fetch(DATA + path).then((r) => {
        if (!r.ok) throw new Error(`${path}: ${r.status}`);
        return r.json();
      });
    }
    return cache[path];
  }

  // ---------- freshness + season status ----------
  function renderFresh() {
    const s = INDEX.sources;
    const tiles = [];
    tiles.push({ lab: 'Dernier avis du NHC traité', big: fmtTime(s.last_advisory_issue_time),
      det: ago(s.last_advisory_issue_time) });
    const gOld = s.chirps_gefs_last_issue_date ? daysOld(s.chirps_gefs_last_issue_date) : 99;
    tiles.push({ lab: 'Dernière prévision de pluie (CHIRPS-GEFS)',
      big: `émise le ${fmtDate(s.chirps_gefs_last_issue_date)}`,
      det: gOld > 2 ? 'En retard : aucune prévision plus récente n’a été reçue.' : 'une prévision par jour',
      stale: gOld > 2 });
    const iOld = s.imerg_last_date ? daysOld(s.imerg_last_date) : 99;
    tiles.push({ lab: 'Dernière pluie observée (IMERG)', big: `jusqu’au ${fmtDate(s.imerg_last_date)}`,
      det: iOld > 3 ? 'En retard : aucune observation plus récente n’a été reçue.' : 'un jour arrive vers 15 h UTC le lendemain',
      stale: iOld > 3 });
    tiles.push({ lab: 'Page mise à jour', big: fmtTime(INDEX.generated_at), det: ago(INDEX.generated_at) });
    $('fresh').innerHTML = tiles.map((t) => `<div class="stat${t.stale ? ' stale' : ''}">
      <div class="lab">${esc(t.lab)}</div><div class="big">${esc(t.big)}</div>
      <div class="det">${esc(t.det)}</div></div>`).join('');
  }

  const stormName = (atcf) => {
    const s = INDEX.storms.find((x) => x.atcf_id === atcf);
    return s ? s.name : atcf;
  };

  function renderStages() {
    $('season').textContent = INDEX.season;
    const T = INDEX.triggers, act = INDEX.season_activations;
    const desc = {
      mobilisation: `≥ ${T.mobilisation.rain_mm} mm prévus sur 2 jours OU > 0 personne exposée aux vents de ${INDEX.exposure_wind_kt} kt, prévisions jusqu’à ${T.mobilisation.lt_max_hrs} h`,
      action: `Mêmes conditions, prévisions jusqu’à ${T.action.lt_max_hrs} h (et alertes DGPC, non suivies ici)`,
      obsv: `≥ ${T.obsv.rain_mm} mm observés sur 2 jours OU > 0 personne exposée aux vents de ${INDEX.exposure_wind_kt} kt observés`,
    };
    $('stages').innerHTML = ['mobilisation', 'action', 'obsv'].map((k) => {
      const a = act[k];
      const who = a ? `${esc(stormName(a.atcf_id))}, ${fmtTime(a.issue_time)}` : '';
      const noSep = a && a.separate_activation === false;
      return `<div class="stage${a ? ' hit' : ''}">
        <div class="nm">${esc(T[k].name_fr)}</div>
        <div class="st">${a ? (noSep ? '✓ Conditions remplies' : '✓ Atteint') : 'Non atteint'}</div>
        <div class="det">${a ? `Premier déclenchement : ${who}` : esc(desc[k])}${noSep
          ? '. Pas d’activation séparée : l’Action avait déjà été déclenchée pour cette tempête.' : ''}</div>
      </div>`;
    }).join('');
  }

  // ---------- storm table + selector ----------
  const nearStorms = () => INDEX.storms.filter((s) => s.min_dist_km != null && s.min_dist_km <= INDEX.info_distance_km);

  function renderStormTable() {
    const only = $('only-near').checked;
    const list = only ? nearStorms() : INDEX.storms;
    const tb = document.querySelector('#storm-table tbody');
    if (!list.length) {
      tb.innerHTML = `<tr><td colspan="8" class="na">Aucune tempête n’est passée à moins de ${num(INDEX.info_distance_km)} km d’Haïti cette saison. Décochez la case pour voir toutes les tempêtes suivies.</td></tr>`;
      return;
    }
    const cell = (a) => (a ? `<td class="hit">✓ ${fmtTimeShort(a.issue_time)}</td>` : '<td class="na">—</td>');
    tb.innerHTML = list.map((s) => `<tr data-atcf="${esc(s.atcf_id)}"${storm && storm.atcf_id === s.atcf_id ? ' class="sel"' : ''}>
      <td><b>${esc(s.name)}</b> <span class="muted small">${esc(s.atcf_id)}</span>${s.active ? '<span class="badge ok">active</span>' : ''}</td>
      <td class="r">${s.n_advisories}</td>
      <td>${fmtTimeShort(s.first_issue_time)}</td><td>${fmtTimeShort(s.last_issue_time)}</td>
      <td class="r">${num(s.min_dist_km)} km</td>
      ${cell(s.activations.mobilisation)}${cell(s.activations.action)}${cell(s.activations.obsv)}
    </tr>`).join('');
    tb.querySelectorAll('tr[data-atcf]').forEach((tr) => tr.addEventListener('click', () => {
      $('storm-select').value = tr.dataset.atcf;
      selectStorm(tr.dataset.atcf, null).then(() => $('avis').scrollIntoView({ behavior: 'smooth' }));
    }));
  }

  function renderStormSelect() {
    const sel = $('storm-select');
    sel.innerHTML = INDEX.storms.map((s) =>
      `<option value="${esc(s.atcf_id)}">${esc(s.name)} (${esc(s.atcf_id)}) · ${num(s.min_dist_km)} km</option>`).join('');
    sel.addEventListener('change', () => selectStorm(sel.value, null));
  }

  function defaultStorm() {
    const near = nearStorms();
    const activeNear = near.filter((s) => s.active).sort((a, b) => a.min_dist_km - b.min_dist_km);
    if (activeNear.length) return activeNear[0].atcf_id;
    if (near.length) return near[0].atcf_id; // storms are sorted newest first
    return INDEX.storms.length ? INDEX.storms[0].atcf_id : null;
  }

  // ---------- map ----------
  const STY = {
    rain_zone: { color: '#1862d8', weight: 1.2, fillColor: '#1862d8', fillOpacity: 0.04 },
    info_zone: { color: '#8a989b', weight: 1, fill: false },
    hti_adm0: { color: '#1e2a2b', weight: 1.4, fillColor: '#1e2a2b', fillOpacity: 0.06 },
    le120: { color: '#c0392b', weight: 1, fillColor: '#f4a0a0', fillOpacity: 0.45 },
    le72: { color: '#9e2a1f', weight: 1.4, fillColor: '#d9534f', fillOpacity: 0.5 },
    swath: { color: '#5b4a73', weight: 1, fillColor: '#8e7cad', fillOpacity: 0.45 },
  };
  const PT = { le72: '#1e2a2b', le120: '#6f7e81', gt120: '#c3ccce' };
  const LAYERS = [
    ['le120', 'Vents ≥ 64 kt prévus, jusqu’à 120 h (Mobilisation)', 'sw', STY.le120],
    ['le72', 'Vents ≥ 64 kt prévus, jusqu’à 72 h (Action)', 'sw', STY.le72],
    ['swath', 'Vents ≥ 64 kt déjà observés', 'sw', STY.swath],
    ['track', 'Trajectoire prévue', 'dot', { color: PT.le72, fillColor: PT.le72 }],
    ['obsv', 'Trajectoire observée', 'line', { color: '#1e2a2b' }],
    ['rain_zone', 'Zone de 230 km (pluie)', 'sw', STY.rain_zone],
    ['info_zone', 'Zone de 1 000 km (courriels)', 'line', STY.info_zone],
  ];

  function initMap() {
    map = L.map('carte', { scrollWheelZoom: false, worldCopyJump: true }).setView([19, -72.5], 6);
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}',
      { attribution: 'Tiles © Esri — Esri, HERE, Garmin, OpenStreetMap contributors', maxZoom: 13 }).addTo(map);
    LAYERS.forEach(([k]) => { layers[k] = L.layerGroup().addTo(map); });
    GEO.features.forEach((f) => {
      const k = f.properties.layer, target = k === 'hti_adm0' ? L.layerGroup().addTo(map) : layers[k];
      L.geoJSON(f, { style: STY[k], interactive: false }).addTo(target);
    });
    $('layer-toggles').innerHTML = LAYERS.map(([k, lab, kind, st]) =>
      `<label><input type="checkbox" data-layer="${k}" checked>
       <span class="sw ${kind === 'sw' ? '' : kind}" style="border-color:${st.color};background:${kind === 'line' ? 'transparent' : (k === 'rain_zone' ? '#e8f0fc' : (st.fillColor || st.color))}"></span>${esc(lab)}</label>`).join('');
    document.querySelectorAll('#layer-toggles input').forEach((cb) => cb.addEventListener('change', () => {
      const l = layers[cb.dataset.layer];
      if (cb.checked) l.addTo(map); else map.removeLayer(l);
    }));
  }

  function drawAdvisoryMap(fc, adv) {
    ['le120', 'le72', 'swath', 'track', 'obsv'].forEach((k) => layers[k].clearLayers());
    const feats = fc ? fc.features : [];
    const bounds = L.latLngBounds([[17.9, -74.5], [20.1, -71.6]]);
    feats.forEach((f) => {
      const p = f.properties;
      if (p.layer === 'fcast_64kt_le120h' || p.layer === 'fcast_64kt_le72h') {
        const k = p.lt_max_hrs === 72 ? 'le72' : 'le120';
        const g = L.geoJSON(f, { style: STY[k] }).bindTooltip(
          `Vents ≥ 64 kt prévus jusqu’à ${p.lt_max_hrs} h<br>${pers(p.pop_exposed_hti)} personnes en Haïti`, { sticky: true });
        g.addTo(layers[k]); bounds.extend(g.getBounds());
      } else if (p.layer === 'obsv_64kt_swath') {
        const g = L.geoJSON(f, { style: STY.swath }).bindTooltip(
          `Vents ≥ 64 kt observés jusqu’au ${fmtTime(p.swath_valid_time)}<br>${pers(p.pop_exposed_hti)} personnes en Haïti`
          + (p.subtracted_from_forecast === false ? '<br><i>Non retirée des zones prévues : pas d’andain à l’heure exacte de l’avis.</i>' : ''), { sticky: true });
        g.addTo(layers.swath); bounds.extend(g.getBounds());
      }
    });
    const pts = feats.filter((f) => f.properties.layer === 'fcast_track')
      .sort((a, b) => a.properties.leadtime_hrs - b.properties.leadtime_hrs);
    if (pts.length) {
      L.polyline(pts.map((f) => [f.geometry.coordinates[1], f.geometry.coordinates[0]]),
        { color: '#6f7e81', weight: 2, interactive: false }).addTo(layers.track);
      pts.forEach((f) => {
        const p = f.properties, ll = [f.geometry.coordinates[1], f.geometry.coordinates[0]];
        const c = p.within_72h ? PT.le72 : (p.within_120h ? PT.le120 : PT.gt120);
        L.circleMarker(ll, { radius: 5, color: '#fff', weight: 2, fillColor: c, fillOpacity: 1 })
          .bindTooltip(`<b>+${num(p.leadtime_hrs)} h</b> · ${fmtTime(p.valid_time)}<br>Vent ${num(p.wind_kt)} kt · ${num(p.dist_km)} km d’Haïti`)
          .addTo(layers.track);
        bounds.extend(ll);
      });
    }
    // observed track up to the advisory
    if (storm && storm.obsv_track) {
      const t0 = dt(adv.issue_time).getTime();
      const o = storm.obsv_track.features.filter((f) => dt(f.properties.valid_time).getTime() <= t0);
      if (o.length) {
        L.polyline(o.map((f) => [f.geometry.coordinates[1], f.geometry.coordinates[0]]),
          { color: '#1e2a2b', weight: 2.5 }).addTo(layers.obsv);
        o.forEach((f) => {
          const p = f.properties, ll = [f.geometry.coordinates[1], f.geometry.coordinates[0]];
          L.circleMarker(ll, { radius: 3, color: '#1e2a2b', weight: 1, fillColor: '#fff', fillOpacity: 1 })
            .bindTooltip(`Observé · ${fmtTime(p.valid_time)}<br>Vent ${num(p.wind_kt)} kt · ${num(p.dist_km)} km d’Haïti`)
            .addTo(layers.obsv);
          bounds.extend(ll);
        });
      }
    }
    map.fitBounds(bounds.pad(0.08), { maxZoom: 7 });
  }

  // ---------- stage cards ----------
  function stageCard(adv, k) {
    const s = adv.stages[k];
    const cut = adv.past_cutoff;
    const res = s.triggered ? '<span class="res met">✓ Déclenché</span>'
      : (cut ? '<span class="badge cut">après l’heure limite</span>' : '<span class="res notmet">Non déclenché</span>');
    let rain;
    if (s.rain_eligible == null) {
      rain = '<span class="notmet">Non disponible</span> pour cet avis.';
    } else if (!s.rain_eligible) {
      rain = `<span class="notmet">Ne compte pas :</span> la tempête n’est pas prévue à moins de ${num(INDEX.rain_distance_km)} km d’Haïti dans les ${s.lt_max_hrs} h.`;
    } else {
      const win = s.rain_window_start ? `, du ${fmtDate(s.rain_window_start)} au ${fmtDate(s.rain_window_end)}` : '';
      rain = `<span class="val">${mm(s.rain_2day_max_mm)}</span> max. sur 2 jours${win} · seuil ${s.rain_threshold_mm} mm · `
        + (s.rain_met ? '<span class="met">remplie</span>' : '<span class="notmet">non remplie</span>');
    }
    const wind = `<span class="val">${pers(s.exp_total)}</span> pers. (${pers(s.exp_fcast)} prévues + ${pers(adv.exp_obsv)} déjà observées) · seuil &gt; 0 · `
      + (s.exp_met ? '<span class="met">remplie</span>' : '<span class="notmet">non remplie</span>');
    return `<div class="scard${s.triggered ? ' hit' : ''}${cut ? ' cut' : ''}">
      <h4><span>${esc(s.name_fr)} <span class="muted small">prévisions ≤ ${s.lt_max_hrs} h</span></span>${res}</h4>
      <div class="row"><b>Pluie</b><div>${rain}</div></div>
      <div class="row"><b>Vents 64 kt</b><div>${wind}</div></div>
      ${k === 'action' ? '<div class="row"><b>DGPC</b><div class="muted">Alertes orange et rouge : non suivies ici.</div></div>' : ''}
    </div>`;
  }

  function renderStageCards(adv) {
    const passIso = adv.closest_pass_time || (adv.hours_to_closest_pass == null ? null
      : new Date(dt(adv.issue_time).getTime() + adv.hours_to_closest_pass * 3.6e6).toISOString());
    const cutTxt = adv.hours_to_closest_pass == null ? '—'
      : `passage au plus près prévu dans <b>${num(adv.hours_to_closest_pass)} h</b> (${fmtTime(passIso)}), à ${num(adv.min_dist_km)} km d’Haïti`;
    // Advisories scored before 2026-09-22 came from the old dev storms DB;
    // the current one stores them under the synoptic time, 3 h earlier.
    const why = adv.issue_time < '2026-09-23'
      ? 'il a été évalué sur l’ancienne base des tempêtes (avant le 22 septembre 2026), où les heures d’émission diffèrent de la base actuelle'
      : 'il n’est plus dans la base des tempêtes sous cette heure d’émission';
    const note = adv.rebuilt && !adv.rebuilt.available
      ? `<p class="small muted">Zones de vents et série de pluie non disponibles pour cet avis : ${why}. Les valeurs sont celles enregistrées par le suivi.</p>` : '';
    $('stage-cards').innerHTML = `<p class="small">${cutTxt}. Heure limite : ${adv.cutoff_hrs} h.</p>${note}`
      + STAGES.map((k) => stageCard(adv, k)).join('');
  }

  // ---------- charts ----------
  const tip = $('tip');
  function showTip(html, ev) {
    tip.innerHTML = html; tip.hidden = false;
    const w = tip.offsetWidth, h = tip.offsetHeight;
    let x = ev.clientX + 14, y = ev.clientY + 14;
    if (x + w > window.innerWidth - 8) x = ev.clientX - w - 14;
    if (y + h > window.innerHeight - 8) y = ev.clientY - h - 14;
    tip.style.left = `${x}px`; tip.style.top = `${y}px`;
  }
  const hideTip = () => { tip.hidden = true; };

  // days: [{date, mean_mm, sum_2day_mm, ...}]; windows: [{key, start, end, fill, label}]
  function rainChart(el, days, opt) {
    if (!days || !days.length) {
      el.innerHTML = `<div class="empty">${esc(opt.empty || 'Pas de données.')}</div>`; return;
    }
    const W = Math.max(320, el.clientWidth - 8), H = 230;
    const m = { l: 42, r: 14, t: 16, b: 38 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b, n = days.length, bw = iw / n;
    const vmax = Math.max(opt.threshold * 1.15, ...days.map((d) => Math.max(d.mean_mm || 0, d.sum_2day_mm || 0)) ) * 1.08;
    const y = (v) => m.t + ih - (v / vmax) * ih;
    const xC = (i) => m.l + bw * (i + 0.5);
    const step = vmax > 150 ? 50 : (vmax > 60 ? 20 : 10);
    let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(opt.aria)}">`;
    // window bands
    (opt.windows || []).forEach((w) => {
      const i0 = days.findIndex((d) => d.date >= w.start);
      let i1 = -1; days.forEach((d, i) => { if (d.date <= w.end) i1 = i; });
      if (i0 < 0 || i1 < i0) return;
      s += `<rect x="${m.l + bw * i0}" y="${m.t}" width="${bw * (i1 - i0 + 1)}" height="${ih}" fill="${w.fill}"/>`;
    });
    // grid
    for (let v = 0; v <= vmax; v += step) {
      s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(v)}" y2="${y(v)}" stroke="#e7edf0" stroke-width="1"/>`
        + `<text x="${m.l - 6}" y="${y(v) + 4}" font-size="11" fill="#5e6a6b" text-anchor="end">${v}</text>`;
    }
    s += `<text x="${m.l - 6}" y="${m.t - 5}" font-size="11" fill="#5e6a6b" text-anchor="end">mm</text>`;
    // daily bars (context)
    const bwi = Math.max(2, Math.min(18, bw * 0.5));
    days.forEach((d, i) => {
      if (d.mean_mm == null) return;
      const h = Math.max(0, y(0) - y(d.mean_mm)), r = Math.min(4, bwi / 2, h);
      const x0 = xC(i) - bwi / 2, y0 = y(d.mean_mm);
      s += `<path d="M${x0},${y(0)} V${y0 + r} Q${x0},${y0} ${x0 + r},${y0} H${x0 + bwi - r} Q${x0 + bwi},${y0} ${x0 + bwi},${y0 + r} V${y(0)} Z" fill="var(--rain-day)"/>`;
    });
    // threshold
    s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(opt.threshold)}" y2="${y(opt.threshold)}" stroke="#1e2a2b" stroke-width="1.5"/>`
      + `<text x="${W - m.r}" y="${y(opt.threshold) - 5}" font-size="11.5" font-weight="700" fill="#1e2a2b" text-anchor="end">seuil ${opt.threshold} mm</text>`;
    // 2-day sum line + markers
    const pts = days.map((d, i) => (d.sum_2day_mm == null ? null : [xC(i), y(d.sum_2day_mm)]));
    let path = '', pen = false;
    pts.forEach((p) => { if (!p) { pen = false; return; } path += `${pen ? 'L' : 'M'}${p[0]},${p[1]} `; pen = true; });
    s += `<path d="${path}" fill="none" stroke="#1862d8" stroke-width="2" stroke-linejoin="round"/>`;
    days.forEach((d, i) => {
      if (!pts[i]) return;
      // red only where the sum counts: over the threshold AND in a window
      const inWin = (opt.windows || []).some((w) => w.start && d.date >= w.start && d.date <= w.end);
      const over = inWin && d.sum_2day_mm >= opt.threshold;
      s += `<circle cx="${pts[i][0]}" cy="${pts[i][1]}" r="4" fill="${over ? '#d03b3b' : '#1862d8'}" stroke="#fff" stroke-width="2"/>`;
    });
    // x labels
    const every = Math.ceil(n / Math.max(4, Math.floor(iw / 52)));
    days.forEach((d, i) => {
      const dd = new Date(d.date + 'T00:00:00Z');
      if (i % every !== 0 && i !== n - 1) return;
      const lab = dd.toLocaleDateString('fr-FR', { timeZone: 'UTC', day: 'numeric', month: 'short' });
      s += `<text x="${xC(i)}" y="${H - m.b + 16}" font-size="11" fill="#5e6a6b" text-anchor="middle">${esc(lab)}</text>`;
    });
    s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(0)}" y2="${y(0)}" stroke="#c3ccce" stroke-width="1"/>`;
    // hover columns
    days.forEach((d, i) => {
      s += `<rect class="hov" data-i="${i}" x="${m.l + bw * i}" y="${m.t}" width="${bw}" height="${ih}" fill="transparent"/>`;
    });
    s += '</svg>';
    const leg = [
      `<span><span class="sw" style="background:var(--rain-day);border-color:var(--rain-day)"></span>Pluie du jour (moyenne nationale)</span>`,
      `<span><span class="sw line" style="border-color:#1862d8"></span>Cumul sur 2 jours (ce qui est comparé au seuil)</span>`,
      `<span><span class="sw line" style="border-color:#1e2a2b"></span>Seuil</span>`,
    ].concat(opt.windows ? ['<span><span class="sw dot" style="background:#d03b3b;border-color:#d03b3b"></span>Au-dessus du seuil, dans une fenêtre</span>'] : [])
      .concat((opt.windows || []).filter((w) => w.start).map((w) =>
      `<span><span class="sw" style="background:${w.fill};border-color:${w.fill}"></span>${esc(w.label)}</span>`));
    el.innerHTML = s + `<div class="legend">${leg.join('')}</div>`;
    el.querySelectorAll('rect.hov').forEach((r) => {
      r.addEventListener('mousemove', (ev) => {
        const d = days[+r.dataset.i];
        const inW = (opt.windows || []).filter((w) => w.start && d.date >= w.start && d.date <= w.end).map((w) => w.short);
        showTip(`<b>${esc(fmtDay(d.date))}</b><br>Pluie du jour : ${mm(d.mean_mm)}<br>Cumul 2 jours : <b>${mm(d.sum_2day_mm)}</b>`
          + (opt.windows ? `<br>${inW.length ? `Compte pour : ${esc(inW.join(', '))}` : 'Hors fenêtre : ne compte pas'}` : ''), ev);
      });
      r.addEventListener('mouseleave', hideTip);
    });
  }

  function drawFcastChart(adv) {
    const g = adv.chirps_gefs;
    if (!g) {
      $('gefs-cap').textContent = 'Série CHIRPS-GEFS non disponible pour cet avis.';
      rainChart($('chart-fcast'), null, { threshold: 68, empty: 'Pas de série.' }); return;
    }
    $('gefs-cap').textContent = `Prévision CHIRPS-GEFS émise le ${fmtDate(g.issue_date)}, la plus récente disponible avant l’avis. `
      + 'Les bandes bleues marquent les jours où la tempête est prévue à moins de 230 km d’Haïti : la pluie ne compte que ces jours-là.';
    const st = adv.stages;
    rainChart($('chart-fcast'), g.days, {
      threshold: st.action.rain_threshold_mm,
      aria: 'Pluie prévue CHIRPS-GEFS pour cet avis',
      windows: [
        { start: st.mobilisation.rain_window_start, end: st.mobilisation.rain_window_end, fill: 'var(--win120)', label: 'Fenêtre ≤ 120 h (Mobilisation)', short: 'Mobilisation' },
        { start: st.action.rain_window_start, end: st.action.rain_window_end, fill: 'var(--win72)', label: 'Fenêtre ≤ 72 h (Action)', short: 'Action' },
      ],
    });
  }

  // ---------- explorer ----------
  async function selectStorm(atcf, wantIdx) {
    if (!atcf) return;
    const my = ++stormReq;
    const s = await getJSON(`storms/${atcf}.json`);
    if (my !== stormReq) return; // a newer storm selection won
    storm = s;
    const advs = storm.advisories;
    const r = $('adv-range');
    r.max = Math.max(0, advs.length - 1);
    advIdx = wantIdx == null ? advs.length - 1 : wantIdx;
    r.value = advIdx;
    renderStormTable();
    renderAdvTable();
    renderObsv();
    await showAdvisory();
  }

  async function showAdvisory() {
    const advs = storm.advisories;
    if (!advs.length) return;
    const row = advs[advIdx], my = ++advReq;
    $('prev').disabled = advIdx <= 0; $('next').disabled = advIdx >= advs.length - 1;
    $('adv-lab').innerHTML = `${esc(storm.name)} · avis ${advIdx + 1} sur ${advs.length} · ${fmtTime(row.issue_time)}`
      + (row.past_cutoff ? '<span class="badge cut">après l’heure limite</span>' : '<span class="badge ok">avant l’heure limite</span>');
    document.querySelectorAll('#adv-table tbody tr').forEach((tr) => tr.classList.toggle('sel', +tr.dataset.i === advIdx));
    let fc;
    try { fc = await getJSON(row.file); } catch (e) { fc = null; }
    if (my !== advReq) return; // a newer advisory selection won
    const adv = fc ? fc.advisory : null;
    if (!adv) { $('stage-cards').innerHTML = '<p class="muted">Fichier de l’avis indisponible.</p>'; return; }
    drawAdvisoryMap(fc, adv);
    renderStageCards(adv);
    drawFcastChart(adv);
  }

  function renderAdvTable() {
    const tb = document.querySelector('#adv-table tbody');
    const res = (r, k) => (r[`${k}_trigger`] ? '<td class="hit">✓ Déclenché</td>' : '<td class="na">—</td>');
    const rain = (v) => (v == null ? '<td class="r na">hors zone</td>' : `<td class="r">${mm(v)}</td>`);
    tb.innerHTML = storm.advisories.map((r, i) => `<tr data-i="${i}">
      <td>${fmtTimeShort(r.issue_time)}</td><td class="r">${num(r.hours_to_closest_pass)} h</td>
      <td>${r.past_cutoff ? '<span class="badge cut">dépassée</span>' : '<span class="badge ok">avant</span>'}</td>
      ${rain(r.rain_120h)}<td class="r">${pers(r.exp_total_120h)}</td>${res(r, 'mobilisation')}
      ${rain(r.rain_72h)}<td class="r">${pers(r.exp_total_72h)}</td>${res(r, 'action')}
      <td><a href="${DATA}${esc(r.file)}" download>GeoJSON</a></td>
    </tr>`).reverse().join('');
    tb.querySelectorAll('tr').forEach((tr) => tr.addEventListener('click', (ev) => {
      if (ev.target.tagName === 'A') return;
      advIdx = +tr.dataset.i; $('adv-range').value = advIdx; showAdvisory();
    }));
  }

  function renderObsv() {
    const o = storm.obsv;
    $('obsv-cap').innerHTML = o.rain_window_start
      ? `IMERG, moyenne nationale. La tempête a été observée à moins de ${num(INDEX.rain_distance_km)} km d’Haïti du ${fmtDate(o.rain_window_start)} au ${fmtDate(o.rain_window_end)} (jour suivant compris) : la pluie observée ne compte que ces jours-là.`
      : `IMERG, moyenne nationale. ${esc(storm.name)} n’a pas été observée à moins de ${num(INDEX.rain_distance_km)} km d’Haïti : la pluie observée ne compte pas pour cette tempête.`;
    rainChart($('chart-obsv'), o.imerg_days, {
      threshold: o.rain_threshold_mm, aria: 'Pluie observée IMERG pendant la tempête',
      empty: 'Pas de jours IMERG pour cette tempête.',
      windows: [{ start: o.rain_window_start, end: o.rain_window_end, fill: 'var(--win72)', label: 'Fenêtre de la pluie observée', short: 'Réponse précoce' }],
    });
    const tb = document.querySelector('#obsv-table tbody');
    if (!o.rows.length) { tb.innerHTML = '<tr><td colspan="5" class="na">Aucun jour observé enregistré.</td></tr>'; return; }
    tb.innerHTML = o.rows.slice().reverse().map((r) => `<tr>
      <td>${fmtDate(r.imerg_through)}</td>
      <td class="r">${r.obsv_rain == null ? '<span class="na">hors zone</span>' : mm(r.obsv_rain)}</td>
      <td>${r.rainfall_relevant ? 'oui' : '<span class="muted">non</span>'}</td>
      <td class="r">${pers(r.obsv_exp)}</td>
      ${r.obsv_trigger ? '<td class="hit">✓ Déclenché</td>' : '<td class="na">—</td>'}
    </tr>`).join('');
  }

  function renderLatestRain() {
    const g = INDEX.chirps_gefs_latest;
    $('gefs-latest-cap').textContent = `Émise le ${fmtDate(g.issue_date)}, moyenne nationale, 16 jours. Seuil de la Mobilisation et de l’Action : ${INDEX.triggers.action.rain_mm} mm sur 2 jours, s’il y a une tempête prévue à moins de ${INDEX.rain_distance_km} km.`;
    rainChart($('chart-gefs-latest'), g.days, { threshold: INDEX.triggers.action.rain_mm, aria: 'Dernière prévision CHIRPS-GEFS' });
    rainChart($('chart-imerg-recent'), INDEX.imerg_recent, { threshold: INDEX.triggers.obsv.rain_mm, aria: 'Pluie observée IMERG récente' });
  }

  function renderExample() {
    const base = new URL(DATA, location.href).href;
    const s = nearStorms()[0] || INDEX.storms[0];
    const id = s ? s.atcf_id : 'AL052026';
    $('example-code').textContent =
`# Tous les avis d’une tempête, puis celui émis à une heure donnée
curl -s ${base}storms/${id}.json | jq '.advisories[] | {issue_time, file}'

import requests
base = "${base}"
storm = requests.get(base + "storms/${id}.json").json()
adv = next(a for a in storm["advisories"] if a["issue_time"] == "${s && s.last_issue_time ? s.last_issue_time : '2026-09-01T15:00:00Z'}")
fc = requests.get(base + adv["file"]).json()      # GeoJSON
print(fc["advisory"]["stages"]["action"])         # pluie, vents, résultat
import geopandas as gpd                           # les zones de vents
gdf = gpd.GeoDataFrame.from_features(fc["features"], crs=4326)`;
  }

  // ---------- boot ----------
  async function boot() {
    try {
      [INDEX, GEO] = await Promise.all([getJSON('index.json'), getJSON('geo/hti.geojson')]);
    } catch (e) {
      $('load-error').hidden = false; console.error(e); return;
    }
    $('mon-start').textContent = fmtDate(INDEX.monitoring_start);
    document.querySelectorAll('.info-km').forEach((e) => { e.textContent = num(INDEX.info_distance_km); });
    document.querySelectorAll('.rain-km').forEach((e) => { e.textContent = num(INDEX.rain_distance_km); });
    renderFresh(); renderStages(); renderStormSelect(); renderStormTable(); renderLatestRain(); renderExample();
    $('only-near').addEventListener('change', renderStormTable);
    initMap();
    $('adv-range').addEventListener('input', (e) => { advIdx = +e.target.value; showAdvisory(); });
    $('prev').addEventListener('click', () => { if (advIdx > 0) { advIdx--; $('adv-range').value = advIdx; showAdvisory(); } });
    $('next').addEventListener('click', () => { if (advIdx < storm.advisories.length - 1) { advIdx++; $('adv-range').value = advIdx; showAdvisory(); } });
    const first = defaultStorm();
    if (first) { $('storm-select').value = first; await selectStorm(first, null); }
    let rt;
    window.addEventListener('resize', () => {
      clearTimeout(rt);
      rt = setTimeout(() => { renderLatestRain(); if (storm) { renderObsv(); showAdvisory(); } }, 200);
    });
  }
  boot();
})();
