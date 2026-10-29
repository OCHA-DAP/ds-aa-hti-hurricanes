# Haiti Anticipatory Action: hurricanes
<!-- markdownlint-disable MD013 -->
[![Generic badge](https://img.shields.io/badge/STATUS-ENDORSED-%231EBFB3)](https://shields.io/)

This repository contains the analysis code and the monitoring pipeline for the Haiti Anticipatory Action framework for hurricanes (2026 version, endorsed). The 2024 framework document is available online here: [Cadre d'Action Anticipatoire : Pilote en Haïti | Tempêtes/Ouragans](https://www.unocha.org/publications/report/haiti/cadre-daction-anticipatoire-pilote-en-haiti-tempetesouragans).

**Site:** <https://ocha-dap.github.io/ds-aa-hti-hurricanes/> — the 2026 version, with every analysis behind it, is at <https://ocha-dap.github.io/ds-aa-hti-hurricanes/2026/>.

## Trigger definition (2026 version)

| Déclencheur | Période de retour | Délai max. des prévisions | Conditions |
|---|---|---|---|
| Prévisions : Mobilisation | ≈ 3,0 ans | 120 h | ≥ 68 mm de précipitations prévues (2 jours) OU > 0 personnes prévues d'être exposées aux vents de 64 nœuds |
| Prévisions : Action | 2,7 ans | 72 h | ≥ 68 mm de précipitations prévues (2 jours) OU > 0 personnes prévues d'être exposées aux vents de 64 nœuds OU alerte orange DGPC dans au moins 6 départements (sur 10)* OU alerte rouge DGPC confirmée par un Hurricane Warning du NHC* |
| Observations : Réponse précoce | 3,4 ans | — | ≥ 57 mm de précipitations observées (2 jours) OU > 0 personnes observées d'être exposées aux vents de 64 nœuds |

Période de retour globale : 2,4 ans (13 storms in 10 seasons, 2002–2025).

**Cutoff**: no forecast trigger may fire once the storm is forecast to make landfall or pass closest to Haiti within 48 hours (informational emails are still sent, flagged as past-cutoff). This includes both DGPC conditions; held to the cutoff, the red + Hurricane Warning condition adds no activation in 2002–2025 (see the [2026 page](https://ocha-dap.github.io/ds-aa-hti-hurricanes/2026/#rouge)).

*The two DGPC conditions are not implemented in the monitoring system: there is no feed of DGPC alerts.

Trigger design and calibration live in `exploration/wsp_trigger.py` (marimo app; run with `uv run marimo edit exploration/wsp_trigger.py`).

## Monitoring

One Databricks job (`databricks.yml`, job `HTI Hurricane Monitoring`) runs `pipelines/monitor.py` (then `pipelines/export_site_data.py` for the public page) at 03:50/09:50/15:50/21:50 UTC — after the upstream `ds-storms-pipeline` (every 3 h) has landed each NHC advisory's tracks, wind-exposure and WSP data in the storms DB (dev).

Data sources:

- **Tracks / wind exposure / WSP**: storms DB (`storms.nhc_tracks_geo`, `storms.nhc_tracks_obsv_exposure`, `storms.nhc_wsp_fcastonly_*`), written by [`ds-storms-pipeline`](https://github.com/OCHA-DAP/ds-storms-pipeline). The leadtime-capped forecast exposure (72 h / 120 h) is recomputed in-repo (`src/monitoring/exposure.py`) with the same method (buffer math from `ocha-lens`, WorldPop 2026 zonal stats via `exactextract`).
- **Forecast rainfall**: CHIRPS-GEFS national-mean 2-day rolling sum, refreshed daily by `.github/workflows/run_update_chirps_gefs.yml` (CHIRPS3-GEFS `c3g` datastream since 2026-07-01, when CHIRPS2-GEFS was discontinued).
- **Observed rainfall**: IMERG national mean (Postgres, prod).

The pipeline back-fills: every advisory/IMERG day is checked exactly once (`monitor_id` dedup); advisories not yet in the storms DB are deferred to the next run. Records:

- `monitoring/hti_fcast_monitoring_v2.parquet` / `monitoring/hti_obsv_monitoring_v2.parquet` — one row per storm × issue time, with per-stage rain/exposure values and trigger booleans. (The v1 files are kept for the historical record.)
- `email/email_record_v2.csv` — one row per email sent (`info`, `mobilisation`, `action`, `obsv`).
- `monitoring/site/` — data for the public monitoring page `/2026/suivi/` (task `export_site`, see [Published site](#published-site)).

### Emails (Listmonk)

Emails are sent through the team Listmonk instance (`ocha-relay` client). Lists (see `pipelines/setup_listmonk_lists.py`):

- `AA Haïti ouragans - informations` (id 116) — every advisory while a storm is within 1 000 km: stage status, WSP exceedance chart, track map.
- `AA Haïti ouragans - déclencheurs` (id 117) — one email per storm per stage when a stage fires.

Send behaviour is controlled by env vars (safe by default): `TEST_EMAIL` (default `True` → internal test list 110) and `DRY_RUN` (default `True` → build but don't send). The deployed job runs with `dry_run=False`; flip `test_email=False` at framework go-live (`databricks bundle deploy -p DEFAULT --var test_email=False`).

### Deployment

```shell
databricks bundle deploy -p DEFAULT          # deploy the prod job
databricks bundle run hti_monitoring -p DEFAULT   # manual run
databricks bundle deploy -t dev -p DEFAULT --var git_branch=my-feature  # feature testing
```

The job clones this repo from GitHub at run time (`source: GIT`), so pushing `main` updates the next run without a redeploy.

### Test email

```shell
uv run python pipelines/send_test_email.py   # Hurricane Melissa (2025) replay to the test list
```

## Directory structure

```shell
.
├── .github/workflows/                  # CHIRPS-GEFS refresh + keep-awake
├── databricks.yml                      # Databricks Asset Bundle (monitoring job)
├── databricks/run_monitor_job.py       # DBX wrapper (secrets, PYTHONPATH)
├── databricks/run_site_export_job.py   # DBX wrapper for the page-data export
├── docs/2026/suivi/                    # public monitoring page (static)
├── exploration/                        # analysis notebooks + marimo apps
├── pipelines/
│   ├── monitor.py                      # main monitoring entrypoint
│   ├── export_site_data.py             # data for the monitoring page
│   ├── sync_site_data.py               # deploy: download + verify that data
│   ├── render_site_static.py           # deploy: status as text (no-JS, llms.txt)
│   ├── send_test_email.py              # Melissa-replay test email
│   ├── setup_listmonk_lists.py         # provision Listmonk lists
│   └── update_chirps_gefs.py           # daily CHIRPS-GEFS refresh
├── src/
│   ├── constants.py                    # trigger thresholds & stage definitions
│   ├── datasources/                    # data loading (incl. storms_db.py)
│   ├── email/
│   │   ├── body.py                     # HTML bodies (French)
│   │   ├── plots.py                    # WSP exceedance chart, storm map
│   │   ├── send.py                     # Listmonk send path
│   │   └── update_emails.py            # decide what's due, dedup, send
│   ├── monitoring/
│   │   ├── exposure.py                 # leadtime-capped wind exposure
│   │   ├── monitoring_utils.py         # trigger evaluation per advisory
│   │   └── site_export.py              # monitoring page files (blob)
│   └── utils/
└── ...
```

## DGPC alert levels (August 2026 guidance)

The DGPC communicated the thresholds behind its orange and red alert levels
(orange: 100 mm/24 h or 100–120 km/h; red: 60–80 mm/h or 300 mm/6–12 h or
≥ 200 km/h). `src/dgpc/` evaluates them against every storm that came within
`D_THRESH` of Haiti, 2002–2025, and publishes the result to the Pages site.

The DGPC did not say whether the values are forecast or observed, which data
sources they refer to, or over what area a threshold must be met — so the
analysis reports the readings side by side rather than picking one. Assumptions
are documented in `src/dgpc/constants.py` and on the page itself.

- **Wind** — NHC forecasts only carry 34/50/64-kt radii, none of which is a DGPC
  level, so `src/dgpc/windfield.py` fits a piecewise power-law profile through
  those radii plus a climatological radius of maximum wind (Willoughby et al.
  2006; NHC does not forecast it). Invariants are guarded by a runnable
  self-check: `uv run python -m src.dgpc.windfield`.
- **Rainfall** — the framework's own rainfall plumbing is daily, which cannot
  address the sub-daily criteria, so `src/datasources/imerg_hh.py` pulls IMERG
  half-hourly (CMR for granule discovery, GES DISC OPeNDAP with a bbox
  constraint for the data). **Needs working Earthdata credentials**
  (`IMERG_USERNAME` / `IMERG_PASSWORD`).

```shell
uv run python pipelines/run_dgpc_wind.py    # ~10 min, writes to blob
uv run python pipelines/build_dgpc_page.py  # renders docs/2026/premiere-evaluation.html
```

The **rainfall** leg runs on Databricks rather than locally, because the
Earthdata credentials live in the `dsci` secret scope and are injected by the
compute policy — nobody has to hold the password (job `dgpc_rain` in
`databricks.yml`; ~45 min for all 42 storms):

```shell
databricks bundle deploy -p DEFAULT
databricks bundle run dgpc_rain -p DEFAULT --params dgpc_storms=AL142016  # smoke test
databricks bundle run dgpc_rain -p DEFAULT                                # all storms
```

A `--storm`/`dgpc_storms` run **merges** into the stored table rather than
replacing it, and the pipeline refuses to write an empty result, so a partial
run cannot wipe the rest. Killing the local CLI does not stop the job — it
runs server-side; poll it with `databricks jobs get-run <run_id>`.

Both numerical modules carry a runnable self-check:

```shell
uv run python -m src.dgpc.windfield       # profile reproduces every NHC radius
uv run python -m src.dgpc.rain_analysis   # aggregation ordering, land masking
```

## Published site

`.github/workflows/deploy-pages.yml` publishes `main:/docs` at
<https://ocha-dap.github.io/ds-aa-hti-hurricanes/>, plus the monitoring
page's data under `/2026/suivi/data/` (downloaded from blob, never committed).
One repo gets one Pages
site, so products share a URL space behind a landing page at `/`
([convention](https://github.com/OCHA-DAP/ds-knowledge-base/blob/main/methods/static-data-apps.md)):

| Path | Product | Built by |
|---|---|---|
| `/` | landing page (monitoring, one card per framework version, the repo) | hand-edited `docs/index.html` + `docs/assets/` |
| `/2026/suivi/` | live monitoring page (French; English at `/2026/suivi/en/`): season status, every NHC advisory on a map with the 64 kt buffers capped at 120 h / 72 h, rain windows, latest CHIRPS-GEFS / IMERG, and public data files | static `docs/2026/suivi/` (HTML/JS/CSS); data from the DBX task `export_site` (`src/monitoring/site_export.py`) → dev blob `ds-aa-hti-hurricanes/monitoring/site/` → `pipelines/sync_site_data.py` in the deploy workflow, then `pipelines/render_site_static.py` writes the current status as plain text into both pages (hidden when JavaScript runs) |
| `/llms.txt`, `/2026/suivi/llms.txt` | guides for scripts and AI assistants: the site, then every monitoring file, which one answers which question, and the easy misreadings; both carry the current status | hand-edited `docs/llms.txt`, `docs/2026/suivi/llms.txt`; status block filled by `pipelines/render_site_static.py` at deploy |
| `/2026/` | the 2026 version on one page: trigger, embedded deck, map replay, threshold choice, DGPC orange and red analyses (French) | `pipelines/build_page_2026.py`; map data in `docs/2026/assets/carte/` from `pipelines/build_dgpc_map.py` (needs the storms DB) |
| `/2026/slides.html` | trigger-mechanism deck (French); PDF in `docs/2026/assets/` | hand-edited; PDF printed with headless Chrome (`--headless=new --no-pdf-header-footer --print-to-pdf`) from the served page |
| `/2026/premiere-evaluation.html` | archived first DGPC alert-level assessment (French) | `pipelines/build_dgpc_page.py` |

### Monitoring page data (`/2026/suivi/data/`)

The DBX job's second task, `export_site`, runs after every monitoring run and
republishes the run's records with the inputs behind them: one GeoJSON per
NHC advisory (forecast track, 64 kt buffers capped at 120 h and 72 h with the
observed swath removed, observed swath, the CHIRPS-GEFS issuance used and each
stage's rain window), one JSON per storm, the monitoring tables as CSV, the
rain series and an `index.json`. Storm and advisory files, and `index.json` for the
latest-rainfall charts, carry each storm's closest distance to Haiti per UTC day
(`dist_days`, from the same 30-min track as the rainfall windows), drawn under
every rainfall chart. Trigger values are always the recorded ones;
geometries are rebuilt with `process_fcast_advisory(detail=True)` and any
difference from the record is written into the file (`rebuilt.differences`).
Advisory files are written once; everything else each run; `manifest.json`
(sha256 per file) is written last and checked by the deploy. The files are
public and documented on the page itself ("Accéder aux données"), which is
how partners (e.g. WFP) get the data: plain HTTPS, no account or key.

```shell
uv run python pipelines/export_site_data.py --local /tmp/site   # dry run to a directory (needs the DB)
uv run python pipelines/export_site_data.py                     # dev blob, new advisories only
uv run python pipelines/export_site_data.py --full               # rewrite every advisory file
```

The September 2026 URLs (`/slides.html`, `/carte-avis.html`,
`/dgpc-departements.html`, `/dgpc-alertes.html`) are redirect stubs into
`/2026/`, and `docs/assets/declencheur-aa-haiti-ouragans.pdf` is a copy of the
2026 deck PDF, so links already shared keep working. The framework document
links to `/2026/`; a later version gets its own folder and card.

French terminology follows the team's doc-sourced glossary
([KB `docs/I18N.md`](https://github.com/OCHA-DAP/ds-knowledge-base/blob/main/docs/I18N.md)).

## Reproducing this analysis

This repo uses [uv](https://docs.astral.sh/uv/):

```shell
uv sync
uv run python pipelines/monitor.py
```

## Development

All code is formatted according to `black` and `flake8` guidelines.
The repo is set-up to use `pre-commit`.
Before you start developing in this repository, you will need to run

```shell
pre-commit install
```

You can run all hooks against all your files using

```shell
pre-commit run --all-files
```

It is also **strongly** recommended to use `jupytext`
to convert all Jupyter notebooks (`.ipynb`) to Markdown files (`.md`)
before committing them into version control.
