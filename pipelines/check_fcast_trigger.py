from src.email import email_utils
from src.monitoring import monitoring_utils

if __name__ == "__main__":
    monitoring_utils.update_fcast_monitoring()
    email_utils.update_fcast_trigger_emails()
