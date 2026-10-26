"""Download the monitoring page's data from dev blob, checked against its manifest.

Used by .github/workflows/deploy-pages.yml to put the files that
src/monitoring/site_export.py wrote (on Databricks) into the Pages
artifact under /suivi/data/. Needs only azure-storage-blob and the dev
blob READ SAS (DSCI_AZ_BLOB_DEV_SAS); it does not import the repo's code.

    python pipelines/sync_site_data.py DEST_DIR

Exits non-zero if the manifest is missing or any file is missing or does
not match its sha256, so a broken export never goes live. The export
writes the manifest last, so a mismatch usually means it was mid-run:
the download is retried a few times before giving up.
"""

import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from azure.storage.blob import ContainerClient

ACCOUNT_URL = "https://imb0chd0dev.blob.core.windows.net/projects"
PREFIX = "ds-aa-hti-hurricanes/monitoring/site"


def download(dest: str) -> list[str]:
    sas = os.environ["DSCI_AZ_BLOB_DEV_SAS"].lstrip("?")
    cc = ContainerClient.from_container_url(f"{ACCOUNT_URL}?{sas}")
    out = Path(dest)

    def get(path: str) -> bytes:
        return cc.get_blob_client(f"{PREFIX}/{path}").download_blob().readall()

    raw = get("manifest.json")
    files = json.loads(raw)["files"]

    def fetch(item):
        path, meta = item
        if path == "manifest.json":
            return None
        try:
            data = get(path)
        except Exception as exc:  # noqa: BLE001 - reported, then retried
            return f"cannot download {path}: {exc.__class__.__name__}"
        if hashlib.sha256(data).hexdigest() != meta["sha256"]:
            return f"sha256 mismatch: {path}"
        target = out / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return None

    with ThreadPoolExecutor(max_workers=16) as pool:
        errors = [e for e in pool.map(fetch, files.items()) if e]
    if not errors:
        (out / "manifest.json").write_bytes(raw)
        print(f"Downloaded {len(files)} files to {out}")
    return errors


def main(dest: str, attempts: int = 4, wait_s: int = 60) -> int:
    for i in range(attempts):
        errors = download(dest)
        if not errors:
            return 0
        print("\n".join(errors), file=sys.stderr)
        if i < attempts - 1:
            print(f"Retrying in {wait_s} s (export may be mid-run)")
            time.sleep(wait_s)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
