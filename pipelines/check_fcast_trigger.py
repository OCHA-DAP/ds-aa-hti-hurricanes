from src.email import plotting, update_emails
from src.monitoring import monitoring_utils

if __name__ == "__main__":
    monitoring_utils.update_fcast_monitoring()
    update_emails.update_fcast_trigger_emails()
    plotting.update_plots(fcast_obsv="fcast")
    update_emails.update_fcast_info_emails()
