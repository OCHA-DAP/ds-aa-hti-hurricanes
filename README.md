# Haiti Anticipatory Action: hurricanes
<!-- markdownlint-disable MD013 -->
[![Generic badge](https://img.shields.io/badge/STATUS-ENDORSED-%231EBFB3)](https://shields.io/)

This repository contains the analysis code and the monitoring pipelines for the Haiti Anticipatory Action framework for hurricanes. The framework document is available online here: [Cadre d’Action Anticipatoire : Pilote en Haïti | Tempêtes/Ouragans
](https://www.unocha.org/publications/report/haiti/cadre-daction-anticipatoire-pilote-en-haiti-tempetesouragans).

## Directory structure

The code in this repository is organized as follows:

```shell
.
├── .github/
│   └── workflows/
│       └── ...                         # GitHub Actions workflows
├── exploration/
│   └── ...                             # Jupyter notebooks for analysis
├── pipelines/
│   ├── check_fcast_trigger.py          # Pipeline to check forecast trigger
│   ├── check_obsv_trigger.py           # Pipeline to check observation trigger
│   └── update_chirps_gefs.py           # Pipeline to update CHIRPS-GEFS data
├── src/
│   ├── datasources/
│   │   └── ...                         # Data loading and processing
│   ├── email/
│   │   ├── static/
│   │   │   └── ...                     # Static files for emails
│   │   ├── templates/
│   │   │   └── ...                     # Email .html templates
│   │   ├── plotting.py                 # Generate and save plots
│   │   ├── send_emails.py              # Send individual emails
│   │   ├── update_emails.py            # Send emails and update sent email record
│   │   └── utils.py                    # Email utility functions
│   ├── monitoring/
│   │   └── monitoring_utils.py         # Functions to update monitoring record
│   ├── utils/
│   │   ├── blob.py                     # Blob storage utilities
│   │   └── raster.py                   # Raster processing utilities
│   └── constants.py
└── ...
```

## Monitoring

This repository contains three pipelines that are used for framework trigger monitoring, all in the `pipelines/` directory, with respective GitHub Actions workflows in the `.github/workflows/` directory.

All pipelines are designed to back-fill. This means that if any step fails during a workflow run, it will be attempted to be re-done during the next run. This ensures that all forecast and observational data is checked against the trigger conditions once.

There are two sets of records that the monitoring pipelines back-fill against (i.e., ensure they are fully up-to-date):

- Monitoring records
  - Files `monitoring/hti_fcast_monitoring.parquet` and `monitoring/hti_obsv_monitoring.parquet`
  - These records have a single row per issue time-storm combination.
  - Each row is produced by processing the relevant data and seeing if it triggered.
- Email records
  - File `email/email_record.csv`
  - This record has a single row per email that has been sent.
  - Emails are either informational (`info`), meaning they are sent every time there is new forecast or observational data for a nearby storm, or trigger (`readiness`, `action`, or `obsv`), meaning they are sent only when trigger conditions are met.

The basic functioning of these pipelines is outlined below.

### Forecast-based trigger

- Script: `pipelines/check_fcast_trigger.py`
- Workflow: `.github/workflows/check_trigger.yml`

This pipeline is triggered by the `ds-nhc-forecast` pipeline, whenever a new NHC track forecast is issued.

Steps:

1. `monitoring_utils.update_fcast_monitoring()` iterates over all NHC forecasts to check if they have been "monitored" or not.
   - Each "monitoring point" corresponds to track forecast issued at a specific time for a specific storm. Each monitoring point corresponds to a single row in the `monitoring/hti_fcast_monitoring.parquet` file in the blob.
   - If the monitoring point is not yet in this file, it is checked against the trigger conditions, and appended to the file.
2. `update_emails.update_fcast_trigger_emails()` iterates over all monitoring points, and sends the trigger emails if it has not yet been sent.
   - If `TEST_STORM` is set to `True`, the system is modified so that a trigger email is forced to be sent using fabricated data. This is to test the system. The specific changes made are:
     - A row is added to the monitoring record that meets the trigger conditions using `src.email.utils.add_test_row_to_monitoring`
     - The row that corresponds to this fabricated test storm is removed from the email record, so the pipeline will send this email even though it's been sent before.
3. `plotting.update_plots()` iterates over all monitoring points, and produces the relevant plot and stores it in the blob, if it doesn't exist yet.
   - This has the same `TEST_STORM` functionality as above
4. `update_emails.update_fcast_info_emails()` iterates over all monitoring points, and sends informational emails.
   - This has the same `TEST_STORM` functionality as above

### Observational trigger

- Script: `pipelines/check_obsv_trigger.py`
- Workflow: `.github/workflows/check_obsv_trigger.yml`

This script is triggered by the old IMERG pipeline (TODO: switch to either schedule or triggering by `ds-raster-stats` pipeline).

Steps:

1. `monitoring_utils.update_obsv_monitoring()` iterates over all NHC observed tracks to check if they have been "monitored" or not.
   - Each "monitoring point" corresponds to observed track point issued at a specific time for a specific storm. Each monitoring point corresponds to a single row in the `monitoring/hti_obsv_monitoring.parquet` file in the blob.
   - If the monitoring point is not yet in this file, it is checked against the trigger conditions, and appended to the file.
   - For the observational monitoring points, there is an additional boolean column `rainfall_relevant`. This is `False` if the storm has left the trigger zone, so we don't consider the rainfall to be associated with the storm anymore, as far as the trigger is concerned.
2. `update_emails.update_obsv_trigger_emails()` iterates over all monitoring points, and sends the trigger emails if it has not yet been sent.
   - If `TEST_STORM` is set to `True`, the system is modified so that a trigger email is forced to be sent using fabricated data. This is to test the system. The specific changes made are:
     - A row is added to the monitoring record that meets the trigger conditions using `src.email.utils.add_test_row_to_monitoring`
     - The row that corresponds to this fabricated test storm is removed from the email record, so the pipeline will send this email even though it's been sent before.
3. `plotting.update_plots()` iterates over all monitoring points, and produces the relevant plot and stores it in the blob, if it doesn't exist yet.
   - This has the same `TEST_STORM` functionality as above.
4. `update_emails.update_obsv_info_emails()` iterates over all monitoring points, and sends informational emails.
   - This has the same `TEST_STORM` functionality as above.
   - Emails are not sent if `rainfall_relevant` is `False`.

### CHIRPS-GEFS

- Script: `update_chirps_gefs.py`
- Workflow: `run_update_chirps_gefs.yml`

This pipeline runs on a schedule to grab the daily CHIRPS-GEFS forecast at 8:50am UTC, which is 10 minutes before the next NHC forecast would be issued.

Steps:

1. `download_recent_chirps_gefs()` clips the COGs from the CHIRPS site to a Haiti bounding box, and saves these clipped rasters to the blob.
2. `process_recent_chirps_gefs()` takes the mean over the whole country for all leadtimes and saves to the blob.

## Reproducing this analysis

First install the required Python packages with:

```shell
pip install -r requirements.txt
```

Then, install the local package in `src` using the command:

```shell
pip install -e .
```

## Development

All code is formatted according to `black` and `flake8` guidelines.
The repo is set-up to use `pre-commit`.
Before you start developing in this repository, you will need to run

```shell
pre-commit install
```

The `markdownlint` hook will require
[Ruby](https://www.ruby-lang.org/en/documentation/installation/)
to be installed on your computer.

You can run all hooks against all your files using

```shell
pre-commit run --all-files
```

It is also **strongly** recommended to use `jupytext`
to convert all Jupyter notebooks (`.ipynb`) to Markdown files (`.md`)
before committing them into version control. This will make for
cleaner diffs (and thus easier code reviews) and will ensure that cell outputs aren't
committed to the repo (which might be problematic if working with sensitive data).
