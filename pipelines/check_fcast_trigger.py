from src.email import email_utils, plotting
from src.monitoring import monitoring_utils

if __name__ == "__main__":
    monitoring_utils.update_fcast_monitoring()
    # email_utils.update_fcast_trigger_emails()
    plotting.update_fcast_plots()
    email_utils.update_fcast_info_emails()
