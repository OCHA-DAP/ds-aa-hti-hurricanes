from src.email import plotting, update_emails
from src.monitoring import monitoring_utils
from src.utils.logging import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting the observational trigger pipeline...")

    # logger.info("Updating IMERG data...")
    # imerg.process_recent_imerg()

    logger.info("Updating observational monitoring data...")
    monitoring_utils.update_obsv_monitoring()

    logger.info("Updating observational trigger emails...")
    update_emails.update_obsv_trigger_emails()

    logger.info("Updating observational plots...")
    plotting.update_plots(fcast_obsv="obsv")

    logger.info("Updating observational informational emails...")
    update_emails.update_obsv_info_emails()
