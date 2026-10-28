// Monitoring page: reads the static files under data/ (written by
// src/monitoring/site_export.py, published by .github/workflows/deploy-pages.yml).
(function () {
  'use strict';
  const $ = (id) => document.getElementById(id);
  // One script for both pages: <html lang> picks the strings below, and the
  // English page (en/, one level down) sets <body data-data="../data/">.
  const LANG = document.documentElement.lang === 'en' ? 'en' : 'fr';
  const LOC = LANG === 'en' ? 'en-GB' : 'fr-FR';
  const DATA = document.body.dataset.data || 'data/';
  const STAGES = ['mobilisation', 'action'];
  const cache = {};
  let INDEX, GEO, storm = null, advIdx = 0, map, layers = {}, stormReq = 0, advReq = 0;

  // ---------- strings ----------
  const STAGE_EN = { mobilisation: 'Mobilisation', action: 'Action', obsv: 'Early response' };
  const stageName = (k) => (LANG === 'en' ? STAGE_EN[k] : INDEX.triggers[k].name_fr);
  const STR = {
    fr: {
      loadErr: (href) => `<h3>Les données de suivi n’ont pas pu être chargées.</h3>
        <p>Réessayez dans quelques minutes. Les fichiers sont aussi accessibles
        directement : <a href="${href}">data/index.json</a>.</p>`,
      ago0: 'il y a moins d’une heure',
      agoH: (h) => `il y a ${h} h`,
      agoD: (d) => `il y a ${d} jours`,
      freshAdv: 'Dernier avis du NHC traité',
      freshGefs: 'Dernière prévision de pluie (CHIRPS-GEFS)',
      issued: (d) => `émise le ${d}`,
      gefsLate: 'En retard : aucune prévision plus récente n’a été reçue.',
      gefsOk: 'une prévision par jour',
      freshImerg: 'Dernière pluie observée (IMERG)',
      through: (d) => `jusqu’au ${d}`,
      imergLate: 'En retard : aucune observation plus récente n’a été reçue.',
      imergOk: 'un jour arrive vers 15 h UTC le lendemain',
      freshPage: 'Page mise à jour',
      descMob: (mm_, kt, h) => `≥ ${mm_} mm prévus sur 2 jours OU > 0 personne exposée aux vents de ${kt} kt, prévisions jusqu’à ${h} h`,
      descAct: (h) => `Mêmes conditions, prévisions jusqu’à ${h} h (et alertes DGPC, non suivies ici)`,
      descObsv: (mm_, kt) => `≥ ${mm_} mm observés sur 2 jours OU > 0 personne exposée aux vents de ${kt} kt observés`,
      condMet: '✓ Conditions remplies',
      reached: '✓ Atteint',
      notReached: 'Non atteint',
      firstHit: 'Premier déclenchement : ',
      noSep: '. Pas d’activation séparée : l’Action avait déjà été déclenchée pour cette tempête.',
      noNear: (km) => `Aucune tempête n’est passée à moins de ${km} km d’Haïti cette saison. Décochez la case pour voir toutes les tempêtes suivies.`,
      layers: {
        le120: 'Vents ≥ 64 kt prévus, jusqu’à 120 h (Mobilisation)',
        le72: 'Vents ≥ 64 kt prévus, jusqu’à 72 h (Action)',
        swath: 'Vents ≥ 64 kt déjà observés',
        track: 'Trajectoire prévue',
        obsv: 'Trajectoire observée',
        rain_zone: 'Zone de 230 km (pluie)',
        info_zone: 'Zone de 1 000 km (courriels)',
      },
      tipFcast: (h, n) => `Vents ≥ 64 kt prévus jusqu’à ${h} h<br>${n} personnes en Haïti`,
      tipSwath: (t, n) => `Vents ≥ 64 kt observés jusqu’au ${t}<br>${n} personnes en Haïti`,
      swathKept: '<br><i>Non retirée des zones prévues : pas d’andain à l’heure exacte de l’avis.</i>',
      tipPt: (kt, km) => `Vent ${kt} kt · ${km} km d’Haïti`,
      observed: 'Observé',
      triggered: '✓ Déclenché',
      notTriggered: 'Non déclenché',
      afterCut: 'après l’heure limite',
      beforeCut: 'avant l’heure limite',
      rainNA: '<span class="notmet">Non disponible</span> pour cet avis.',
      rainOff: (km, h) => `<span class="notmet">Ne compte pas :</span> la tempête n’est pas prévue à moins de ${km} km d’Haïti dans les ${h} h.`,
      win: (a, b) => `, du ${a} au ${b}`,
      rainVal: (v, win, thr) => `<span class="val">${v}</span> max. sur 2 jours${win} · seuil ${thr} mm · `,
      windVal: (tot, f, o) => `<span class="val">${tot}</span> pers. (${f} prévues + ${o} déjà observées) · seuil &gt; 0 · `,
      met: 'remplie',
      notMet: 'non remplie',
      fcastLe: (h) => `prévisions ≤ ${h} h`,
      rowRain: 'Pluie',
      rowWind: 'Vents 64 kt',
      dgpcRow: 'Alertes orange et rouge : non suivies ici.',
      pass: (h, t, km) => `passage au plus près prévu dans <b>${h} h</b> (${t}), à ${km} km d’Haïti`,
      whyOld: 'il a été évalué sur l’ancienne base des tempêtes (avant le 22 septembre 2026), où les heures d’émission diffèrent de la base actuelle',
      whyGone: 'il n’est plus dans la base des tempêtes sous cette heure d’émission',
      unavail: (why) => `Zones de vents et série de pluie non disponibles pour cet avis : ${why}. Les valeurs sont celles enregistrées par le suivi.`,
      cutoff: (h) => `Heure limite : ${h} h.`,
      noData: 'Pas de données.',
      thr: (t) => `seuil ${t} mm`,
      legDay: 'Pluie du jour (moyenne nationale)',
      leg2: 'Cumul sur 2 jours (ce qui est comparé au seuil)',
      legThr: 'Seuil',
      legOver: 'Au-dessus du seuil, dans une fenêtre',
      tipDay: 'Pluie du jour : ',
      tip2: 'Cumul 2 jours : ',
      countsFor: 'Compte pour : ',
      outside: 'Hors fenêtre : ne compte pas',
      gefsNA: 'Série CHIRPS-GEFS non disponible pour cet avis.',
      noSeries: 'Pas de série.',
      gefsCap: (d) => `Prévision CHIRPS-GEFS émise le ${d}, la plus récente disponible avant l’avis. `
        + 'Les bandes bleues marquent les jours où la tempête est prévue à moins de 230 km d’Haïti : la pluie ne compte que ces jours-là.',
      ariaFcast: 'Pluie prévue CHIRPS-GEFS pour cet avis',
      win120: 'Fenêtre ≤ 120 h (Mobilisation)',
      win72: 'Fenêtre ≤ 72 h (Action)',
      advOf: (i, n) => `avis ${i} sur ${n}`,
      advFileNA: 'Fichier de l’avis indisponible.',
      outZone: 'hors zone',
      cutPassed: 'dépassée',
      cutBefore: 'avant',
      obsvIn: (km, a, b) => `IMERG, moyenne nationale. La tempête a été observée à moins de ${km} km d’Haïti du ${a} au ${b} (jour suivant compris) : la pluie observée ne compte que ces jours-là.`,
      obsvOut: (name, km) => `IMERG, moyenne nationale. ${name} n’a pas été observée à moins de ${km} km d’Haïti : la pluie observée ne compte pas pour cette tempête.`,
      ariaObsv: 'Pluie observée IMERG pendant la tempête',
      noImerg: 'Pas de jours IMERG pour cette tempête.',
      obsvWin: 'Fenêtre de la pluie observée',
      distTitle: 'Distance à Haïti (point le plus proche du jour), km',
      legDist: 'Distance de la tempête à Haïti',
      legNear: (km) => `À moins de ${km} km : la pluie compte`,
      legFar: (km) => `À plus de ${km} km`,
      tipDist: (n, km) => `${n} : ${km} km`,
      dayAfter: (km) => `Aucune tempête à moins de ${km} km ce jour-là, mais encore dans la fenêtre (elle inclut le jour suivant)`,
      noActive: 'Aucune tempête active : la pluie prévue ne peut pas déclencher en ce moment.',
      noObsStorm: 'Aucune tempête observée sur ces jours.',
      distNA: 'Distance de la tempête non disponible.',
      distOld: ' Distance de la tempête non disponible pour cet avis.',
      latestAdv: (list) => ` Fenêtres et distances : dernier avis du NHC de chaque tempête active (${list}).`,
      noObsvRows: 'Aucun jour observé enregistré.',
      yes: 'oui',
      no: 'non',
      latestCap: (d, thr, km) => `Émise le ${d}, moyenne nationale, 16 jours. Seuil de la Mobilisation et de l’Action : ${thr} mm sur 2 jours, s’il y a une tempête prévue à moins de ${km} km.`,
      ariaGefs: 'Dernière prévision CHIRPS-GEFS',
      ariaImerg: 'Pluie observée IMERG récente',
      exSeason: '# Toute la saison : valeurs enregistrées et zones de vents exactes',
      exStorm: '# Tous les avis d’une tempête, puis celui émis à une heure donnée',
      exStages: '# pluie, vents, résultat',
      exZones: '# les zones de vents',
    },
    en: {
      loadErr: (href) => `<h3>The monitoring data could not be loaded.</h3>
        <p>Try again in a few minutes. The files can also be read directly:
        <a href="${href}">data/index.json</a>.</p>`,
      ago0: 'less than an hour ago',
      agoH: (h) => `${h} h ago`,
      agoD: (d) => `${d} days ago`,
      freshAdv: 'Latest NHC advisory processed',
      freshGefs: 'Latest rainfall forecast (CHIRPS-GEFS)',
      issued: (d) => `issued ${d}`,
      gefsLate: 'Late: no newer forecast has arrived.',
      gefsOk: 'one forecast a day',
      freshImerg: 'Latest observed rainfall (IMERG)',
      through: (d) => `through ${d}`,
      imergLate: 'Late: no newer observation has arrived.',
      imergOk: 'each day arrives around 15:00 UTC the next day',
      freshPage: 'Page updated',
      descMob: (mm_, kt, h) => `≥ ${mm_} mm forecast over 2 days OR > 0 people exposed to ${kt} kt winds, forecasts up to ${h} h`,
      descAct: (h) => `Same conditions, forecasts up to ${h} h (plus DGPC alerts, not monitored here)`,
      descObsv: (mm_, kt) => `≥ ${mm_} mm observed over 2 days OR > 0 people exposed to observed ${kt} kt winds`,
      condMet: '✓ Conditions met',
      reached: '✓ Reached',
      notReached: 'Not reached',
      firstHit: 'First triggered: ',
      noSep: '. No separate activation: Action had already been triggered for this storm.',
      noNear: (km) => `No storm has come within ${km} km of Haiti this season. Untick the box to see every monitored storm.`,
      layers: {
        le120: 'Forecast winds ≥ 64 kt, up to 120 h (Mobilisation)',
        le72: 'Forecast winds ≥ 64 kt, up to 72 h (Action)',
        swath: 'Winds ≥ 64 kt already observed',
        track: 'Forecast track',
        obsv: 'Observed track',
        rain_zone: '230 km zone (rainfall)',
        info_zone: '1,000 km zone (emails)',
      },
      tipFcast: (h, n) => `Forecast winds ≥ 64 kt up to ${h} h<br>${n} people in Haiti`,
      tipSwath: (t, n) => `Winds ≥ 64 kt observed through ${t}<br>${n} people in Haiti`,
      swathKept: '<br><i>Not removed from the forecast zones: no swath at the exact advisory time.</i>',
      tipPt: (kt, km) => `Wind ${kt} kt · ${km} km from Haiti`,
      observed: 'Observed',
      triggered: '✓ Triggered',
      notTriggered: 'Not triggered',
      afterCut: 'after the cutoff',
      beforeCut: 'before the cutoff',
      rainNA: '<span class="notmet">Not available</span> for this advisory.',
      rainOff: (km, h) => `<span class="notmet">Does not count:</span> the storm is not forecast within ${km} km of Haiti in the next ${h} h.`,
      win: (a, b) => `, ${a} to ${b}`,
      rainVal: (v, win, thr) => `<span class="val">${v}</span> highest 2-day total${win} · threshold ${thr} mm · `,
      windVal: (tot, f, o) => `<span class="val">${tot}</span> people (${f} forecast + ${o} already observed) · threshold &gt; 0 · `,
      met: 'met',
      notMet: 'not met',
      fcastLe: (h) => `forecasts ≤ ${h} h`,
      rowRain: 'Rainfall',
      rowWind: '64 kt winds',
      dgpcRow: 'Orange and red alerts: not monitored here.',
      pass: (h, t, km) => `closest pass forecast in <b>${h} h</b> (${t}), ${km} km from Haiti`,
      whyOld: 'it was scored on the old storms database (before 22 September 2026), where issue times differ from the current database',
      whyGone: 'it is no longer in the storms database under this issue time',
      unavail: (why) => `Wind zones and rainfall series not available for this advisory: ${why}. The values are those the monitoring recorded.`,
      cutoff: (h) => `Cutoff: ${h} h.`,
      noData: 'No data.',
      thr: (t) => `threshold ${t} mm`,
      legDay: 'Daily rainfall (national mean)',
      leg2: '2-day total (what is compared with the threshold)',
      legThr: 'Threshold',
      legOver: 'Above the threshold, in a window',
      tipDay: 'Daily rainfall: ',
      tip2: '2-day total: ',
      countsFor: 'Counts for: ',
      outside: 'Outside the window: does not count',
      gefsNA: 'CHIRPS-GEFS series not available for this advisory.',
      noSeries: 'No series.',
      gefsCap: (d) => `CHIRPS-GEFS forecast issued ${d}, the latest available before the advisory. `
        + 'The blue bands mark the days the storm is forecast within 230 km of Haiti: rainfall only counts on those days.',
      ariaFcast: 'CHIRPS-GEFS forecast rainfall for this advisory',
      win120: 'Window ≤ 120 h (Mobilisation)',
      win72: 'Window ≤ 72 h (Action)',
      advOf: (i, n) => `advisory ${i} of ${n}`,
      advFileNA: 'Advisory file not available.',
      outZone: 'outside zone',
      cutPassed: 'passed',
      cutBefore: 'before',
      obsvIn: (km, a, b) => `IMERG, national mean. The storm was observed within ${km} km of Haiti from ${a} to ${b} (following day included): observed rainfall only counts on those days.`,
      obsvOut: (name, km) => `IMERG, national mean. ${name} was not observed within ${km} km of Haiti: observed rainfall does not count for this storm.`,
      ariaObsv: 'IMERG observed rainfall during the storm',
      noImerg: 'No IMERG days for this storm.',
      obsvWin: 'Observed rainfall window',
      distTitle: 'Distance to Haiti (closest point of the day), km',
      legDist: 'Storm distance to Haiti',
      legNear: (km) => `Within ${km} km: rainfall counts`,
      legFar: (km) => `More than ${km} km away`,
      tipDist: (n, km) => `${n}: ${km} km`,
      dayAfter: (km) => `No storm within ${km} km this day, but still in the window (it includes the day after)`,
      noActive: 'No active storm: forecast rainfall cannot trigger now.',
      noObsStorm: 'No storm observed over these days.',
      distNA: 'Storm distance not available.',
      distOld: ' Storm distance not available for this advisory.',
      latestAdv: (list) => ` Windows and distances: latest NHC advisory of each active storm (${list}).`,
      noObsvRows: 'No observed days recorded.',
      yes: 'yes',
      no: 'no',
      latestCap: (d, thr, km) => `Issued ${d}, national mean, 16 days. Mobilisation and Action threshold: ${thr} mm over 2 days, if a storm is forecast within ${km} km.`,
      ariaGefs: 'Latest CHIRPS-GEFS forecast',
      ariaImerg: 'Recent IMERG observed rainfall',
      exSeason: '# Whole season: recorded values and exact wind zones',
      exStorm: '# Every advisory of one storm, then the one issued at a given time',
      exStages: '# rainfall, winds, result',
      exZones: '# the wind zones',
    },
  }[LANG];

  // ---------- formatting ----------
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g,
    (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const num = (v, d = 0) => (v == null ? '—'
    : Number(v).toLocaleString(LOC, { maximumFractionDigits: d, minimumFractionDigits: d }));
  const pers = (v) => (v == null ? '—' : num(v, 0));
  const mm = (v) => (v == null ? '—' : `${num(v, 1)} mm`);
  const dt = (iso) => (iso ? new Date(iso) : null);
  const fmtTime = (iso) => {
    const d = dt(iso); if (!d) return '—';
    return d.toLocaleString(LOC, { timeZone: 'UTC', day: 'numeric', month: 'short',
      year: 'numeric', hour: '2-digit', minute: '2-digit' }) + ' UTC';
  };
  const fmtTimeShort = (iso) => {
    const d = dt(iso); if (!d) return '—';
    return d.toLocaleString(LOC, { timeZone: 'UTC', day: 'numeric', month: 'short',
      hour: '2-digit', minute: '2-digit' });
  };
  const fmtDate = (s) => {
    if (!s) return '—';
    return new Date(s + 'T00:00:00Z').toLocaleDateString(LOC,
      { timeZone: 'UTC', day: 'numeric', month: 'short', year: 'numeric' });
  };
  const fmtDay = (s) => new Date(s + 'T00:00:00Z').toLocaleDateString(LOC,
    { timeZone: 'UTC', weekday: 'short', day: 'numeric', month: 'short' });
  const ago = (iso) => {
    const h = (Date.now() - dt(iso).getTime()) / 3.6e6;
    if (h < 1) return STR.ago0;
    if (h < 48) return STR.agoH(Math.round(h));
    return STR.agoD(Math.round(h / 24));
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
    tiles.push({ lab: STR.freshAdv, big: fmtTime(s.last_advisory_issue_time),
      det: ago(s.last_advisory_issue_time) });
    const gOld = s.chirps_gefs_last_issue_date ? daysOld(s.chirps_gefs_last_issue_date) : 99;
    tiles.push({ lab: STR.freshGefs,
      big: STR.issued(fmtDate(s.chirps_gefs_last_issue_date)),
      det: gOld > 2 ? STR.gefsLate : STR.gefsOk,
      stale: gOld > 2 });
    const iOld = s.imerg_last_date ? daysOld(s.imerg_last_date) : 99;
    tiles.push({ lab: STR.freshImerg, big: STR.through(fmtDate(s.imerg_last_date)),
      det: iOld > 3 ? STR.imergLate : STR.imergOk,
      stale: iOld > 3 });
    tiles.push({ lab: STR.freshPage, big: fmtTime(INDEX.generated_at), det: ago(INDEX.generated_at) });
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
      mobilisation: STR.descMob(T.mobilisation.rain_mm, INDEX.exposure_wind_kt, T.mobilisation.lt_max_hrs),
      action: STR.descAct(T.action.lt_max_hrs),
      obsv: STR.descObsv(T.obsv.rain_mm, INDEX.exposure_wind_kt),
    };
    $('stages').innerHTML = ['mobilisation', 'action', 'obsv'].map((k) => {
      const a = act[k];
      const who = a ? `${esc(stormName(a.atcf_id))}, ${fmtTime(a.issue_time)}` : '';
      const noSep = a && a.separate_activation === false;
      return `<div class="stage${a ? ' hit' : ''}">
        <div class="nm">${esc(stageName(k))}</div>
        <div class="st">${a ? (noSep ? STR.condMet : STR.reached) : STR.notReached}</div>
        <div class="det">${a ? `${STR.firstHit}${who}` : esc(desc[k])}${noSep ? STR.noSep : ''}</div>
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
      tb.innerHTML = `<tr><td colspan="8" class="na">${STR.noNear(num(INDEX.info_distance_km))}</td></tr>`;
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
    ['le120', STR.layers.le120, 'sw', STY.le120],
    ['le72', STR.layers.le72, 'sw', STY.le72],
    ['swath', STR.layers.swath, 'sw', STY.swath],
    ['track', STR.layers.track, 'dot', { color: PT.le72, fillColor: PT.le72 }],
    ['obsv', STR.layers.obsv, 'line', { color: '#1e2a2b' }],
    ['rain_zone', STR.layers.rain_zone, 'sw', STY.rain_zone],
    ['info_zone', STR.layers.info_zone, 'line', STY.info_zone],
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
          STR.tipFcast(p.lt_max_hrs, pers(p.pop_exposed_hti)), { sticky: true });
        g.addTo(layers[k]); bounds.extend(g.getBounds());
      } else if (p.layer === 'obsv_64kt_swath') {
        const g = L.geoJSON(f, { style: STY.swath }).bindTooltip(
          STR.tipSwath(fmtTime(p.swath_valid_time), pers(p.pop_exposed_hti))
          + (p.subtracted_from_forecast === false ? STR.swathKept : ''), { sticky: true });
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
          .bindTooltip(`<b>+${num(p.leadtime_hrs)} h</b> · ${fmtTime(p.valid_time)}<br>${STR.tipPt(num(p.wind_kt), num(p.dist_km))}`)
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
            .bindTooltip(`${STR.observed} · ${fmtTime(p.valid_time)}<br>${STR.tipPt(num(p.wind_kt), num(p.dist_km))}`)
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
    const res = s.triggered ? `<span class="res met">${STR.triggered}</span>`
      : (cut ? `<span class="badge cut">${STR.afterCut}</span>` : `<span class="res notmet">${STR.notTriggered}</span>`);
    const metTxt = (ok) => (ok ? `<span class="met">${STR.met}</span>` : `<span class="notmet">${STR.notMet}</span>`);
    let rain;
    if (s.rain_eligible == null) {
      rain = STR.rainNA;
    } else if (!s.rain_eligible) {
      rain = STR.rainOff(num(INDEX.rain_distance_km), s.lt_max_hrs);
    } else {
      const win = s.rain_window_start ? STR.win(fmtDate(s.rain_window_start), fmtDate(s.rain_window_end)) : '';
      rain = STR.rainVal(mm(s.rain_2day_max_mm), win, s.rain_threshold_mm) + metTxt(s.rain_met);
    }
    const wind = STR.windVal(pers(s.exp_total), pers(s.exp_fcast), pers(adv.exp_obsv)) + metTxt(s.exp_met);
    return `<div class="scard${s.triggered ? ' hit' : ''}${cut ? ' cut' : ''}">
      <h4><span>${esc(stageName(k))} <span class="muted small">${STR.fcastLe(s.lt_max_hrs)}</span></span>${res}</h4>
      <div class="row"><b>${STR.rowRain}</b><div>${rain}</div></div>
      <div class="row"><b>${STR.rowWind}</b><div>${wind}</div></div>
      ${k === 'action' ? `<div class="row"><b>DGPC</b><div class="muted">${STR.dgpcRow}</div></div>` : ''}
    </div>`;
  }

  function renderStageCards(adv) {
    const passIso = adv.closest_pass_time || (adv.hours_to_closest_pass == null ? null
      : new Date(dt(adv.issue_time).getTime() + adv.hours_to_closest_pass * 3.6e6).toISOString());
    const cutTxt = adv.hours_to_closest_pass == null ? '—'
      : STR.pass(num(adv.hours_to_closest_pass), fmtTime(passIso), num(adv.min_dist_km));
    // Advisories scored before 2026-09-22 came from the old dev storms DB;
    // the current one stores them under the synoptic time, 3 h earlier.
    const why = adv.issue_time < '2026-09-23' ? STR.whyOld : STR.whyGone;
    const note = adv.rebuilt && !adv.rebuilt.available
      ? `<p class="small muted">${STR.unavail(why)}</p>` : '';
    $('stage-cards').innerHTML = `<p class="small">${cutTxt}. ${STR.cutoff(adv.cutoff_hrs)}</p>${note}`
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
  // storms: [{name, dist: [{date, km}]}] adds a panel under the bars with
  // each storm's closest distance to Haiti that day (undefined: no panel,
  // as with files exported before it); noStorms: its text when empty.
  const DIST = '#7a3e9d', FAR = '#9aa5a8';
  function rainChart(el, days, opt) {
    if (!days || !days.length) {
      el.innerHTML = `<div class="empty">${esc(opt.empty || STR.noData)}</div>`; return;
    }
    const storms = opt.storms, KM = INDEX.info_distance_km, LIM = INDEX.rain_distance_km;
    const DG = 32, DH = 124, STRIP = 16; // gap, distance panel, its "> 1 000 km" band
    const W = Math.max(320, el.clientWidth - 8), H = 230 + (storms ? DG + DH : 0);
    const m = { l: 42, r: 14, t: 16, b: 38 };
    const iw = W - m.l - m.r, ih = 230 - m.t - m.b, n = days.length, bw = iw / n;
    const bh = ih + (storms ? DG + DH : 0); // window bands and hover span both panels
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
      s += `<rect x="${m.l + bw * i0}" y="${m.t}" width="${bw * (i1 - i0 + 1)}" height="${bh}" fill="${w.fill}"/>`;
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
      + `<text x="${W - m.r}" y="${y(opt.threshold) - 5}" font-size="11.5" font-weight="700" fill="#1e2a2b" text-anchor="end">${STR.thr(opt.threshold)}</text>`;
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
    s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(0)}" y2="${y(0)}" stroke="#c3ccce" stroke-width="1"/>`;
    // distance panel: 0 to 1 000 km, farther storms in a band on top
    const byDay = days.map(() => []);
    let anyFar = false, drawnAny = false;
    if (storms) {
      const dt = m.t + ih + DG;
      const yk = (km) => (km > KM ? dt + STRIP / 2 : dt + STRIP + (1 - km / KM) * (DH - STRIP));
      const at = new Map(days.map((d, i) => [d.date, i]));
      s += `<rect x="${m.l}" y="${dt}" width="${iw}" height="${STRIP}" fill="#f4f7f9"/>`
        + `<text x="${m.l}" y="${dt - 8}" font-size="11" font-weight="700" fill="#5e6a6b">${esc(STR.distTitle)}</text>`
        + `<text x="${m.l - 6}" y="${dt + STRIP / 2 + 4}" font-size="11" fill="#5e6a6b" text-anchor="end">&gt;${num(KM)}</text>`;
      [0, KM / 2, KM].forEach((v) => {
        s += `<line x1="${m.l}" x2="${W - m.r}" y1="${yk(v)}" y2="${yk(v)}" stroke="#e7edf0" stroke-width="1"/>`
          + (v < KM ? `<text x="${m.l - 6}" y="${yk(v) + 4}" font-size="11" fill="#5e6a6b" text-anchor="end">${num(v)}</text>` : '');
      });
      s += `<line x1="${m.l}" x2="${W - m.r}" y1="${yk(LIM)}" y2="${yk(LIM)}" stroke="#1e2a2b" stroke-width="1.5"/>`
        + `<text x="${W - m.r}" y="${yk(LIM) - 5}" font-size="11.5" font-weight="700" fill="#1e2a2b" text-anchor="end">${num(LIM)} km</text>`;
      let drawn = 0;
      storms.forEach((st) => {
        const sp = (st.dist || []).filter((d) => d.km != null && at.has(d.date)).map((d) => ({ i: at.get(d.date), km: d.km }));
        sp.forEach((p) => { byDay[p.i].push({ name: st.name, km: p.km }); });
        drawn += sp.length;
        let line = '';
        sp.forEach((p, k) => {
          if (p.km > KM) return;
          const prev = sp[k - 1];
          line += `${prev && prev.i === p.i - 1 && prev.km <= KM ? 'L' : 'M'}${xC(p.i)},${yk(p.km)} `;
        });
        if (line) s += `<path d="${line}" fill="none" stroke="${DIST}" stroke-width="2" stroke-linejoin="round"/>`;
        sp.forEach((p) => {
          if (p.km > KM) { anyFar = true; s += `<circle cx="${xC(p.i)}" cy="${yk(p.km)}" r="3" fill="${FAR}"/>`; return; }
          const near = p.km < LIM; // filled where rainfall counts
          s += `<circle cx="${xC(p.i)}" cy="${yk(p.km)}" r="4" fill="${near ? DIST : '#fff'}" stroke="${near ? '#fff' : DIST}" stroke-width="2"/>`;
        });
        const close = sp.filter((p) => p.km <= KM).sort((a, b) => a.km - b.km)[0];
        if (close) {
          const lx = Math.min(Math.max(xC(close.i), m.l + 30), W - m.r - 30);
          s += `<text x="${lx}" y="${yk(close.km) - 9}" font-size="11" font-weight="700" fill="#1e2a2b" text-anchor="middle" paint-order="stroke" stroke="#fff" stroke-width="3">${esc(st.name)}</text>`;
        }
      });
      drawnAny = drawn > 0;
      if (!drawn) {
        s += `<text x="${m.l + iw / 2}" y="${dt + STRIP + (DH - STRIP) / 2 + 4}" font-size="12" fill="#5e6a6b" text-anchor="middle" paint-order="stroke" stroke="#fff" stroke-width="3">${esc(opt.noStorms || STR.distNA)}</text>`;
      }
      s += `<line x1="${m.l}" x2="${W - m.r}" y1="${yk(0)}" y2="${yk(0)}" stroke="#c3ccce" stroke-width="1"/>`;
    }
    // x labels
    const every = Math.ceil(n / Math.max(4, Math.floor(iw / 52)));
    days.forEach((d, i) => {
      const dd = new Date(d.date + 'T00:00:00Z');
      if (i % every !== 0 && i !== n - 1) return;
      const lab = dd.toLocaleDateString(LOC, { timeZone: 'UTC', day: 'numeric', month: 'short' });
      s += `<text x="${xC(i)}" y="${H - m.b + 16}" font-size="11" fill="#5e6a6b" text-anchor="middle">${esc(lab)}</text>`;
    });
    // hover columns
    days.forEach((d, i) => {
      s += `<rect class="hov" data-i="${i}" x="${m.l + bw * i}" y="${m.t}" width="${bw}" height="${bh}" fill="transparent"/>`;
    });
    s += '</svg>';
    const seen = new Set();
    const leg = [
      `<span><span class="sw" style="background:var(--rain-day);border-color:var(--rain-day)"></span>${STR.legDay}</span>`,
      `<span><span class="sw line" style="border-color:#1862d8"></span>${STR.leg2}</span>`,
      `<span><span class="sw line" style="border-color:#1e2a2b"></span>${STR.legThr}</span>`,
    ].concat(opt.windows ? [`<span><span class="sw dot" style="background:#d03b3b;border-color:#d03b3b"></span>${STR.legOver}</span>`] : [])
      .concat((opt.windows || []).filter((w) => w.start && !seen.has(w.label) && seen.add(w.label)).map((w) =>
      `<span><span class="sw" style="background:${w.fill};border-color:${w.fill}"></span>${esc(w.label)}</span>`))
      .concat(storms && drawnAny ? [
        `<span><span class="sw line" style="border-color:${DIST}"></span>${STR.legDist}</span>`,
        `<span><span class="sw dot" style="background:${DIST};border-color:${DIST}"></span>${STR.legNear(num(LIM))}</span>`,
      ] : [])
      .concat(anyFar ? [`<span><span class="sw dot" style="background:${FAR};border-color:${FAR}"></span>${STR.legFar(num(KM))}</span>`] : []);
    el.innerHTML = s + `<div class="legend">${leg.join('')}</div>`;
    el.querySelectorAll('rect.hov').forEach((r) => {
      r.addEventListener('mousemove', (ev) => {
        const i = +r.dataset.i, d = days[i];
        const inW = (opt.windows || []).filter((w) => w.start && d.date >= w.start && d.date <= w.end).map((w) => w.short);
        const near = byDay[i].slice().sort((a, b) => a.km - b.km);
        showTip(`<b>${esc(fmtDay(d.date))}</b><br>${STR.tipDay}${mm(d.mean_mm)}<br>${STR.tip2}<b>${mm(d.sum_2day_mm)}</b>`
          + near.map((p) => `<br>${STR.tipDist(esc(p.name), num(p.km))}`).join('')
          + (opt.windows ? `<br>${inW.length ? `${STR.countsFor}${esc(inW.join(', '))}` : STR.outside}` : '')
          + (storms && inW.length && !near.some((p) => p.km < LIM) ? `<br>${STR.dayAfter(num(LIM))}` : ''), ev);
      });
      r.addEventListener('mouseleave', hideTip);
    });
  }

  function drawFcastChart(adv) {
    const g = adv.chirps_gefs;
    if (!g) {
      $('gefs-cap').textContent = STR.gefsNA;
      rainChart($('chart-fcast'), null, { threshold: 68, empty: STR.noSeries }); return;
    }
    // dist_days: advisory files written before the distance panel lack it
    $('gefs-cap').textContent = STR.gefsCap(fmtDate(g.issue_date)) + (adv.dist_days ? '' : STR.distOld);
    rainChart($('chart-fcast'), g.days, {
      threshold: adv.stages.action.rain_threshold_mm,
      aria: STR.ariaFcast,
      windows: fcastWindows(adv.stages),
      storms: adv.dist_days && [{ name: adv.name, dist: adv.dist_days }],
    });
  }

  // Rainfall windows as chart bands; `who` names the storm when several can show.
  const fcastWindows = (st, who) => ['mobilisation', 'action'].map((k) => ({
    start: st[k].rain_window_start, end: st[k].rain_window_end,
    fill: k === 'action' ? 'var(--win72)' : 'var(--win120)',
    label: k === 'action' ? STR.win72 : STR.win120,
    short: stageName(k) + (who ? ` (${who})` : ''),
  }));
  const obsvWindow = (o, who) => ({
    start: o.rain_window_start, end: o.rain_window_end, fill: 'var(--win72)',
    label: STR.obsvWin, short: stageName('obsv') + (who ? ` (${who})` : ''),
  });

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
    $('adv-lab').innerHTML = `${esc(storm.name)} · ${STR.advOf(advIdx + 1, advs.length)} · ${fmtTime(row.issue_time)}`
      + (row.past_cutoff ? `<span class="badge cut">${STR.afterCut}</span>` : `<span class="badge ok">${STR.beforeCut}</span>`);
    document.querySelectorAll('#adv-table tbody tr').forEach((tr) => tr.classList.toggle('sel', +tr.dataset.i === advIdx));
    let fc;
    try { fc = await getJSON(row.file); } catch (e) { fc = null; }
    if (my !== advReq) return; // a newer advisory selection won
    const adv = fc ? fc.advisory : null;
    if (!adv) { $('stage-cards').innerHTML = `<p class="muted">${STR.advFileNA}</p>`; return; }
    drawAdvisoryMap(fc, adv);
    renderStageCards(adv);
    drawFcastChart(adv);
  }

  function renderAdvTable() {
    const tb = document.querySelector('#adv-table tbody');
    const res = (r, k) => (r[`${k}_trigger`] ? `<td class="hit">${STR.triggered}</td>` : '<td class="na">—</td>');
    const rain = (v) => (v == null ? `<td class="r na">${STR.outZone}</td>` : `<td class="r">${mm(v)}</td>`);
    tb.innerHTML = storm.advisories.map((r, i) => `<tr data-i="${i}">
      <td>${fmtTimeShort(r.issue_time)}</td><td class="r">${num(r.hours_to_closest_pass)} h</td>
      <td>${r.past_cutoff ? `<span class="badge cut">${STR.cutPassed}</span>` : `<span class="badge ok">${STR.cutBefore}</span>`}</td>
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
      ? STR.obsvIn(num(INDEX.rain_distance_km), fmtDate(o.rain_window_start), fmtDate(o.rain_window_end))
      : STR.obsvOut(esc(storm.name), num(INDEX.rain_distance_km));
    rainChart($('chart-obsv'), o.imerg_days, {
      threshold: o.rain_threshold_mm, aria: STR.ariaObsv,
      empty: STR.noImerg,
      windows: [obsvWindow(o)],
      storms: o.dist_days && [{ name: storm.name, dist: o.dist_days }],
    });
    const tb = document.querySelector('#obsv-table tbody');
    if (!o.rows.length) { tb.innerHTML = `<tr><td colspan="5" class="na">${STR.noObsvRows}</td></tr>`; return; }
    tb.innerHTML = o.rows.slice().reverse().map((r) => `<tr>
      <td>${fmtDate(r.imerg_through)}</td>
      <td class="r">${r.obsv_rain == null ? `<span class="na">${STR.outZone}</span>` : mm(r.obsv_rain)}</td>
      <td>${r.rainfall_relevant ? STR.yes : `<span class="muted">${STR.no}</span>`}</td>
      <td class="r">${pers(r.obsv_exp)}</td>
      ${r.obsv_trigger ? `<td class="hit">${STR.triggered}</td>` : '<td class="na">—</td>'}
    </tr>`).join('');
  }

  function renderLatestRain() {
    const g = INDEX.chirps_gefs_latest;
    // g.storms / imerg_recent_storms: absent from an index.json exported
    // before the distance panel; the charts are then drawn without it.
    const fs = g.storms, os = INDEX.imerg_recent_storms;
    $('gefs-latest-cap').textContent = STR.latestCap(fmtDate(g.issue_date), INDEX.triggers.action.rain_mm, INDEX.rain_distance_km)
      + (fs && fs.length ? STR.latestAdv(fs.map((f) => `${f.name}, ${fmtTime(f.issue_time)}${f.past_cutoff ? `, ${STR.afterCut}` : ''}`).join(LANG === 'fr' ? ' ; ' : '; ')) : '');
    rainChart($('chart-gefs-latest'), g.days, {
      threshold: INDEX.triggers.action.rain_mm, aria: STR.ariaGefs,
      windows: fs && fs.flatMap((f) => fcastWindows(f.stages, f.name)),
      storms: fs && fs.map((f) => ({ name: f.name, dist: f.dist_days })),
      noStorms: STR.noActive,
    });
    rainChart($('chart-imerg-recent'), INDEX.imerg_recent, {
      threshold: INDEX.triggers.obsv.rain_mm, aria: STR.ariaImerg,
      windows: os && os.map((o) => obsvWindow(o, o.name)),
      storms: os && os.map((o) => ({ name: o.name, dist: o.dist_days })),
      noStorms: STR.noObsStorm,
    });
  }

  function renderExample() {
    const base = new URL(DATA, location.href).href;
    const s = nearStorms()[0] || INDEX.storms[0];
    const id = s ? s.atcf_id : 'AL052026';
    $('example-code').textContent =
`${STR.exSeason}
import io, requests, pandas as pd, geopandas as gpd
base = "${base}"
rows = pd.read_parquet(base + "fcast_monitoring.parquet")
get = lambda f: io.BytesIO(requests.get(base + f).content)
zones = gpd.read_parquet(get("buffers/fcast_64kt.parquet"))
action = zones[(zones.stage == "action") & (zones.layer == "fcastonly")]

${STR.exStorm}
curl -s ${base}storms/${id}.json | jq '.advisories[] | {issue_time, file}'

import requests
base = "${base}"
storm = requests.get(base + "storms/${id}.json").json()
adv = next(a for a in storm["advisories"] if a["issue_time"] == "${s && s.last_issue_time ? s.last_issue_time : '2026-09-01T15:00:00Z'}")
fc = requests.get(base + adv["file"]).json()      # GeoJSON
print(fc["advisory"]["stages"]["action"])         ${STR.exStages}
import geopandas as gpd                           ${STR.exZones}
gdf = gpd.GeoDataFrame.from_features(fc["features"], crs=4326)`;
  }

  // ---------- boot ----------
  // The status written at deploy (#summary, hidden by style.css when
  // JavaScript runs) is the only one left if the page cannot draw its own.
  const showSummary = () => { if ($('summary')) $('summary').classList.add('show'); };
  async function boot() {
    try {
      [INDEX, GEO] = await Promise.all([getJSON('index.json'), getJSON('geo/hti.geojson')]);
    } catch (e) {
      $('load-error').innerHTML = STR.loadErr(DATA + 'index.json');
      $('load-error').hidden = false;
      showSummary(); console.error(e); return;
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
  boot().catch((e) => { showSummary(); console.error(e); });
})();
