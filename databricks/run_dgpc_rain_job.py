"""DBX entry wrapper for the DGPC rainfall analysis.

Same glue pattern as ``run_monitor_job.py``: keep ``pipelines/`` and
``src/`` pure Python, put the DBX-specific bits here.

Why this runs on Databricks at all: the analysis pulls ~14 000 IMERG
half-hourly granules from GES DISC, which needs Earthdata credentials.
The team's working credentials live in the ``dsci`` secret scope and are
injected by the compute policy as ``IMERG_USERNAME`` / ``IMERG_PASSWORD``
(the same pair the `Run IMERG` job in ds-raster-pipelines uses), so the
job authenticates without anyone handling the password locally.

The bundle passes the job parameters positionally:

    sys.argv[1] = storms   # "" for all, or "AL142016 AL132025"
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
STORMS = _arg(1, "").strip()

# The policy normally injects these; fall back to the secret scope so the
# job still works on compute where it does not.
if not os.environ.get("IMERG_USERNAME") or not os.environ.get(
    "IMERG_PASSWORD"
):
    from databricks.sdk.runtime import dbutils

    for _key in ("IMERG_USERNAME", "IMERG_PASSWORD"):
        try:
            os.environ[_key] = dbutils.secrets.get("dsci", _key)
        except Exception as exc:  # noqa: BLE001
            print(
                f"[run_dgpc_rain_job] WARNING: dsci/{_key} unavailable "
                f"({exc}); the IMERG fetch will fail."
            )

env = dict(os.environ)
env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
# Writable config dirs (the repo clone is on a read-only mount, and
# create_auth_files() writes ~/.netrc).
env["MPLCONFIGDIR"] = "/tmp/mplconfig"
env.setdefault("HOME", "/tmp/dgpc-home")
os.makedirs(env["HOME"], exist_ok=True)

cmd = [
    sys.executable,
    os.path.join(REPO_ROOT, "pipelines", "run_dgpc_rain.py"),
]
if STORMS:
    cmd += ["--storm", *STORMS.split()]

if __name__ == "__main__":
    print(f"[run_dgpc_rain_job] repo_root={REPO_ROOT} storms={STORMS or 'ALL'}")
    rc = subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=False).returncode
    # DBX treats a top-level SystemExit (even 0) as failure; raise only
    # on non-zero.
    if rc != 0:
        raise RuntimeError(f"run_dgpc_rain.py exited with code {rc}")
    print("[run_dgpc_rain_job] OK")
