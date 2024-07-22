from src.email import email, plotting
from src.monitoring import monitoring_utils

if __name__ == "__main__":
    monitoring_utils.update_fcast_monitoring()
    email.update_fcast_trigger_emails()
    plotting.update_fcast_plots()
    email.update_fcast_info_emails()
