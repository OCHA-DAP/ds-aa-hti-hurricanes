from src.datasources import imerg
from src.email import plotting, update_emails
from src.monitoring import monitoring_utils

if __name__ == "__main__":
    imerg.process_recent_imerg()
    monitoring_utils.update_obsv_monitoring()
    update_emails.update_obsv_trigger_emails()
    plotting.update_plots(fcast_obsv="obsv")
