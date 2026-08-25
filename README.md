# Haiti Anticipatory Action: hurricanes
<!-- markdownlint-disable MD013 -->
[![Generic badge](https://img.shields.io/badge/STATUS-PENDING_ENDORSEMENT-%23F2645A)](https://shields.io/)

This repository contains the analysis code and the monitoring pipeline for the Haiti Anticipatory Action framework for hurricanes (2026 revision, pending endorsement). The endorsed 2024 framework document is available online here: [Cadre d'Action Anticipatoire : Pilote en Haïti | Tempêtes/Ouragans](https://www.unocha.org/publications/report/haiti/cadre-daction-anticipatoire-pilote-en-haiti-tempetesouragans).

## Trigger definition (2026 revision)

| Déclencheur | Période de retour | Délai max. des prévisions | Conditions |
|---|---|---|---|
| Prévisions : Mobilisation | ≈ 3,0 ans | 120 h | ≥ 68 mm de précipitations prévues (2 jours) OU > 0 personnes prévues d'être exposées aux vents de 64 kt |
| Prévisions : Action | 3,0 ans | 72 h | ≥ 68 mm de précipitations prévues (2 jours) OU > 0 personnes prévues d'être exposées aux vents de 64 kt OU alerte rouge DGPC confirmée par un Hurricane Warning du NHC* |
| Observations : Réponse précoce | 3,4 ans | — | ≥ 57 mm de précipitations observées (2 jours) OU > 0 personnes observées d'être exposées aux vents de 64 kt |

Période de retour globale : 2,4 ans.

**Cutoff**: no forecast trigger may fire once the storm is forecast to make landfall or pass closest to Haiti within 48 hours (informational emails are still sent, flagged as past-cutoff).

*The DGPC red alert condition is not yet implemented in the monitoring system.

Trigger design and calibration live in `exploration/wsp_trigger.py` (marimo app; run with `uv run marimo edit exploration/wsp_trigger.py`).

## Monitoring

One Databricks job (`databricks.yml`, job `HTI Hurricane Monitoring`) runs `pipelines/monitor.py` at 03:50/09:50/15:50/21:50 UTC — after the upstream `ds-storms-pipeline` (every 3 h) has landed each NHC advisory's tracks, wind-exposure and WSP data in the storms DB (dev).

Data sources:

- **Tracks / wind exposure / WSP**: storms DB (`storms.nhc_tracks_geo`, `storms.nhc_tracks_obsv_exposure`, `storms.nhc_wsp_fcastonly_*`), written by [`ds-storms-pipeline`](https://github.com/OCHA-DAP/ds-storms-pipeline). The leadtime-capped forecast exposure (72 h / 120 h) is recomputed in-repo (`src/monitoring/exposure.py`) with the same method (buffer math from `ocha-lens`, WorldPop 2026 zonal stats via `exactextract`).
- **Forecast rainfall**: CHIRPS-GEFS national-mean 2-day rolling sum, refreshed daily by `.github/workflows/run_update_chirps_gefs.yml` (CHIRPS3-GEFS `c3g` datastream since 2026-07-01, when CHIRPS2-GEFS was discontinued).
- **Observed rainfall**: IMERG national mean (Postgres, prod).

The pipeline back-fills: every advisory/IMERG day is checked exactly once (`monitor_id` dedup); advisories not yet in the storms DB are deferred to the next run. Records:

- `monitoring/hti_fcast_monitoring_v2.parquet` / `monitoring/hti_obsv_monitoring_v2.parquet` — one row per storm × issue time, with per-stage rain/exposure values and trigger booleans. (The v1 files are kept for the historical record.)
- `email/email_record_v2.csv` — one row per email sent (`info`, `mobilisation`, `action`, `obsv`).

### Emails (Listmonk)

Emails are sent through the team Listmonk instance (`ocha-relay` client). Lists (see `pipelines/setup_listmonk_lists.py`):

- `AA Haïti ouragans - informations` (id 116) — every advisory while a storm is within 1 000 km: stage status, WSP exceedance chart, track map.
- `AA Haïti ouragans - déclencheurs` (id 117) — one email per storm per stage when a stage fires.

Send behaviour is controlled by env vars (safe by default): `TEST_EMAIL` (default `True` → internal test list 110) and `DRY_RUN` (default `True` → build but don't send). The deployed job runs with `dry_run=False`; flip `test_email=False` at framework go-live (`databricks bundle deploy -p default --var test_email=False`).

### Deployment

```shell
databricks bundle deploy -p default          # deploy the prod job
databricks bundle run hti_monitoring -p default   # manual run
databricks bundle deploy -t dev -p default --var git_branch=my-feature  # feature testing
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
├── exploration/                        # analysis notebooks + marimo apps
├── pipelines/
│   ├── monitor.py                      # main monitoring entrypoint
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
│   │   └── monitoring_utils.py         # trigger evaluation per advisory
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
uv run python pipelines/run_dgpc_rain.py    # needs Earthdata credentials
uv run python pipelines/build_dgpc_page.py  # renders docs/dgpc-alertes.html
```

## Published site

GitHub Pages serves `main:/docs` at
<https://ocha-dap.github.io/ds-aa-hti-hurricanes/>. One repo gets one Pages
site, so products share a URL space behind a landing page at `/`
([convention](https://github.com/OCHA-DAP/ds-knowledge-base/blob/main/methods/static-data-apps.md)):

| Path | Product | Built by |
|---|---|---|
| `/` | landing page (cards) | hand-edited `docs/index.html` |
| `/slides.html` | trigger-mechanism deck (French) | hand-edited |
| `/dgpc-alertes.html` | DGPC alert-level analysis (French) | `pipelines/build_dgpc_page.py` |

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
