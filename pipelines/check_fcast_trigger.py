from src.email import plotting, update_emails
from src.monitoring import monitoring_utils
from src.utils.logging import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting the forecast trigger pipeline...")

    logger.info("Updating forecast monitoring data...")
    monitoring_utils.update_fcast_monitoring()

    logger.info("Updating forecast trigger emails...")
    update_emails.update_fcast_trigger_emails()

    logger.info("Updating forecast plots...")
    plotting.update_plots(fcast_obsv="fcast")

    logger.info("Updating forecast information emails...")
    update_emails.update_fcast_info_emails()
