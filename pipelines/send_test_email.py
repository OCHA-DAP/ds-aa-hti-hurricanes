"""Send a test monitoring email by replaying Hurricane Melissa (2025).

Runs a real historical NHC advisory through the exact production code
path (trigger evaluation, plots, HTML body, Listmonk send) and sends
the result to the internal test list. Nothing is written to the
monitoring or email records.

Usage:
    python pipelines/send_test_email.py                  # send info email
    python pipelines/send_test_email.py --preview        # write html, no send
    python pipelines/send_test_email.py --issued-time 2025-10-26T18:00
"""

import argparse
import os
import webbrowser
from pathlib import Path

# Send to the framework's own lists (currently Tristan-only, pending
# endorsement) rather than the shared internal test list, with an
# explicit [TEST] label added below. Real send unless DRY_RUN is set.
os.environ["TEST_EMAIL"] = "False"
os.environ.setdefault("DRY_RUN", "False")

import pandas as pd  # noqa: E402

from src.constants import STAGE_NAMES_FR  # noqa: E402
from src.datasources import codab, storms_db  # noqa: E402
from src.email import body, send, update_emails  # noqa: E402
from src.email.plots import fr_datetime  # noqa: E402
from src.monitoring import monitoring_utils  # noqa: E402
from src.utils.logging import get_logger  # noqa: E402

logger = get_logger(__name__)

DEFAULT_ATCF_ID = "al132025"  # Melissa
# pre-cutoff advisories as Melissa approached Haiti
CANDIDATE_ISSUANCES = [
    "2025-10-26T18:00",
    "2025-10-26T12:00",
    "2025-10-26T06:00",
    "2025-10-27T00:00",
    "2025-10-25T18:00",
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--issued-time", default=None)
    parser.add_argument("--atcf-id", default=DEFAULT_ATCF_ID)
    parser.add_argument("--preview", action="store_true")
    args = parser.parse_args()

    adm0 = codab.load_codab_from_blob().to_crs(3857)
    df_gefs_all = monitoring_utils.load_gefs_with_issue_times()

    candidates = (
        [args.issued_time] if args.issued_time else CANDIDATE_ISSUANCES
    )
    storm_name = storms_db.fetch_storm_name(args.atcf_id)
    row = None
    for cand in candidates:
        issue_time = pd.Timestamp(cand)
        logger.info(f"Evaluating {storm_name} advisory {issue_time}...")
        r = monitoring_utils.process_fcast_advisory(
            args.atcf_id,
            issue_time,
            df_gefs_all,
            adm0,
            name=f"{storm_name} (TEST)",
        )
        if r is None:
            continue
        row = pd.Series(r)
        logger.info(
            f"  mobilisation={r['mobilisation_trigger']} "
            f"action={r['action_trigger']} "
            f"past_cutoff={r['past_cutoff']} "
            f"exp_72h={r['exp_total_72h']:.0f} "
            f"exp_120h={r['exp_total_120h']:.0f} "
            f"rain_72h={r['rain_72h']} rain_120h={r['rain_120h']}"
        )
        if r["mobilisation_trigger"] or r["action_trigger"]:
            break
    if row is None:
        raise SystemExit("No usable advisory found in the DB.")

    logger.info("Building plots and body...")
    # Replayed storms are not in the v2 monitoring records, so derive
    # the activated stages from the replayed advisory itself.
    activated = [s for s in ("mobilisation", "action") if row[f"{s}_trigger"]]
    wsp_img, map_img, det_img, rain_img = update_emails.build_email_plots(
        row["atcf_id"], row["issue_time"], row["name"]
    )
    html = body.build_fcast_info_body(
        row,
        wsp_img,
        map_img,
        det_img,
        rain_img,
        activated_fr=[STAGE_NAMES_FR[s] for s in activated],
    )
    subject = (
        f"[TEST] Action anticipatoire Haïti – {row['name']} : "
        f"prévisions NHC du {fr_datetime(row['issue_time'])} "
        f"({update_emails._info_status(row, activated)})"
    )

    if args.preview:
        out = Path("temp/test_email_preview.html")
        out.parent.mkdir(exist_ok=True)
        out.write_text(html)
        logger.info(f"Preview written to {out}")
        webbrowser.open(out.resolve().as_uri())
    else:
        cid = send.send_campaign(
            f"aa-hti-test-{row['monitor_id']}", subject, html, "info"
        )
        logger.info(f"Test email sent (campaign {cid}).")
