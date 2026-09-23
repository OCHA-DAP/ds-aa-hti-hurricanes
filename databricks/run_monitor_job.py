"""DBX entry wrapper for the Haiti hurricane monitoring pipeline.

The bundle's ``spark_python_task`` passes the job parameters positionally:

    sys.argv[1] = test_email    # "True" | "False"
    sys.argv[2] = dry_run       # "True" | "False"
    sys.argv[3] = email_backend # "listmonk" | "ses" (see src/email/ses_mail.py)

``pipelines/monitor.py`` and ``src/`` stay pure Python. This wrapper is
the only DBX-specific glue (same pattern as ds-storms-alerts):

1. Inject the Listmonk credentials from the ``dsci`` secret scope (the
   reused cluster carries DSCI_AZ_* env vars but not the Listmonk ones)
   and export the run-mode env vars.
2. Shell out to ``pipelines/monitor.py`` with PYTHONPATH set to the
   repo root so ``from src ...`` resolves (source: GIT clone, not
   pip-installed).
"""

import os
import subprocess
import sys


def _find_script_dir() -> str:
    """spark_python_task's exec context doesn't always define __file__."""
    try:
        return os.path.dirname(os.path.abspath(__file__))  # noqa: F821
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        return os.path.dirname(os.path.abspath(sys.argv[0]))
    return os.getcwd()


def _arg(i: int, default: str = "") -> str:
    return sys.argv[i] if len(sys.argv) > i else default


REPO_ROOT = os.path.abspath(os.path.join(_find_script_dir(), ".."))

TEST_EMAIL = _arg(1, "True")
DRY_RUN = _arg(2, "True")
EMAIL_BACKEND = _arg(3, "listmonk")

from databricks.sdk.runtime import dbutils  # noqa: E402

# The SES backend needs DSCI_AWS_EMAIL_* instead — same scope, same tolerance.
for _key in (
    "DSCI_LISTMONK_BASE_URL",
    "DSCI_LISTMONK_API_USERNAME",
    "DSCI_LISTMONK_API_KEY",
    "DSCI_AWS_EMAIL_HOST",
    "DSCI_AWS_EMAIL_ADDRESS",
    "DSCI_AWS_EMAIL_USERNAME",
    "DSCI_AWS_EMAIL_PASSWORD",
):
    try:
        os.environ[_key] = dbutils.secrets.get("dsci", _key)
    except Exception as exc:  # noqa: BLE001
        print(
            f"[run_monitor_job] WARNING: dsci/{_key} unavailable ({exc}); "
            "sends via that backend will fail until it is set."
        )

os.environ["TEST_EMAIL"] = TEST_EMAIL
os.environ["DRY_RUN"] = DRY_RUN
os.environ["EMAIL_BACKEND"] = EMAIL_BACKEND

env = dict(os.environ)
env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
# Writable matplotlib config dir (repo clone is on a read-only mount).
env["MPLCONFIGDIR"] = "/tmp/mplconfig"

cmd = [sys.executable, os.path.join(REPO_ROOT, "pipelines", "monitor.py")]

if __name__ == "__main__":
    print(
        f"[run_monitor_job] repo_root={REPO_ROOT} "
        f"TEST_EMAIL={TEST_EMAIL} DRY_RUN={DRY_RUN} EMAIL_BACKEND={EMAIL_BACKEND}"
    )
    rc = subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=False).returncode
    # DBX treats a top-level SystemExit (even 0) as failure; raise only
    # on non-zero.
    if rc != 0:
        raise RuntimeError(f"monitor.py exited with code {rc}")
    print("[run_monitor_job] OK")
