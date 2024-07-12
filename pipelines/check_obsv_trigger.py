from src.datasources import imerg
from src.monitoring import monitoring_utils

if __name__ == "__main__":
    imerg.process_recent_imerg()
    monitoring_utils.update_obsv_monitoring()
