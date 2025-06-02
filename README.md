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

All code is formatted according to black and flake8 guidelines.
The repo is set-up to use pre-commit.
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
