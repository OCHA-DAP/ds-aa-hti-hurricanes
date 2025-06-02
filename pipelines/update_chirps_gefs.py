from src.datasources.chirps_gefs import (
    download_recent_chirps_gefs,
    process_recent_chirps_gefs,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting the CHIRPS-GEFS update pipeline...")
    logger.info("Downloading recent CHIRPS-GEFS data...")
    download_recent_chirps_gefs()
    logger.info("Processing recent CHIRPS-GEFS data...")
    process_recent_chirps_gefs()
