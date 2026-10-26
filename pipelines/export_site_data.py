"""Export the monitoring page's data (see src/monitoring/site_export.py).

Usage:
    python pipelines/export_site_data.py              # dev blob, new advisories only
    python pipelines/export_site_data.py --full       # rewrite every advisory file
    python pipelines/export_site_data.py --local out  # write to a directory instead

Runs on Databricks after each monitoring run (task ``export_site``); the
Pages workflow then publishes the files under /suivi/data/.
"""

import argparse

from src.monitoring import site_export

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--local", default=None)
    args = parser.parse_args()
    site_export.export_site(local_dir=args.local, full=args.full)
