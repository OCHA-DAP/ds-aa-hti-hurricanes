"""Main monitoring entrypoint: update monitoring data, send due emails.

Usage:
    python pipelines/monitor.py            # both fcast + obsv
    python pipelines/monitor.py --fcast    # forecast side only
    python pipelines/monitor.py --obsv     # observational side only

Send behaviour is controlled by the TEST_EMAIL / DRY_RUN env vars
(both default to the safe value; see src/email/send.py).
"""

import argparse

from src.email import update_emails
from src.monitoring import monitoring_utils
from src.utils.logging import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fcast", action="store_true")
    parser.add_argument("--obsv", action="store_true")
    args = parser.parse_args()
    run_fcast = args.fcast or not (args.fcast or args.obsv)
    run_obsv = args.obsv or not (args.fcast or args.obsv)

    if run_fcast:
        logger.info("Updating forecast monitoring data...")
        monitoring_utils.update_fcast_monitoring()
        logger.info("Updating forecast trigger emails...")
        update_emails.update_fcast_trigger_emails()
        logger.info("Updating forecast info emails...")
        update_emails.update_fcast_info_emails()

    if run_obsv:
        logger.info("Updating observational monitoring data...")
        monitoring_utils.update_obsv_monitoring()
        logger.info("Updating observational trigger emails...")
        update_emails.update_obsv_trigger_emails()
        logger.info("Updating observational info emails...")
        update_emails.update_obsv_info_emails()

    logger.info("Done.")
