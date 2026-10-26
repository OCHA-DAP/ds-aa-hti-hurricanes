"""DBX entry wrapper for the monitoring page's data export.

Runs as task ``export_site`` of the ``HTI Hurricane Monitoring`` job, after
``run_monitoring`` on the same job cluster, and shells out to
``pipelines/export_site_data.py`` (new advisories only; the rest is
rewritten every run). The DSCI_AZ_* blob/DB credentials come from the
compute policy; nothing else is needed. See src/monitoring/site_export.py.
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


REPO_ROOT = os.path.abspath(os.path.join(_find_script_dir(), ".."))

env = dict(os.environ)
env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")

cmd = [
    sys.executable,
    os.path.join(REPO_ROOT, "pipelines", "export_site_data.py"),
]

if __name__ == "__main__":
    print(f"[run_site_export_job] repo_root={REPO_ROOT}")
    rc = subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=False).returncode
    # DBX treats a top-level SystemExit (even 0) as failure; raise only
    # on non-zero.
    if rc != 0:
        raise RuntimeError(f"export_site_data.py exited with code {rc}")
    print("[run_site_export_job] OK")
