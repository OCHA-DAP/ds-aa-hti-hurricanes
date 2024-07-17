import os
import smtplib
import ssl
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
from typing import Literal

import pandas as pd
import pytz
from html2text import html2text
from jinja2 import Environment, FileSystemLoader

from src.constants import FRENCH_MONTHS
from src.monitoring import monitoring_utils
from src.utils import blob

EMAIL_HOST = os.getenv("CHD_DS_HOST")
EMAIL_PORT = int(os.getenv("CHD_DS_PORT"))
EMAIL_PASSWORD = os.getenv("CHD_DS_EMAIL_PASSWORD")
EMAIL_USERNAME = os.getenv("CHD_DS_EMAIL_USERNAME")
EMAIL_ADDRESS = os.getenv("CHD_DS_EMAIL_ADDRESS")
TEST_LIST = os.getenv("TEST_LIST")
if TEST_LIST == "False":
    TEST_LIST = False
else:
    TEST_LIST = True
TEST_STORM = os.getenv("TEST_STORM")
if TEST_STORM == "False":
    TEST_STORM = False
else:
    TEST_STORM = True

TEMPLATES_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "templates"
STATIC_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "static"


def add_test_row_to_monitoring(
    df_monitoring: pd.DataFrame, fcast_obsv: Literal["fcast", "obsv"]
) -> pd.DataFrame:
    """Add test row to monitoring df to simulate new monitoring point.
    This new monitoring point will cause an activation of all three triggers.
    """
    print("adding test row to monitoring data")
    if fcast_obsv == "fcast":
        df_monitoring_test = df_monitoring[
            df_monitoring["monitor_id"] == "al022024_fcast_2024-07-01T15:00:00"
        ].copy()
        df_monitoring_test[
            [
                "monitor_id",
                "name",
                "atcf_id",
                "readiness_trigger",
                "action_trigger",
            ]
        ] = (
            "TEST_MONITOR_ID",
            "TEST_STORM_NAME",
            "TEST_ATCF_ID",
            True,
            True,
        )
        df_monitoring = pd.concat(
            [df_monitoring, df_monitoring_test], ignore_index=True
        )
    else:
        df_monitoring_test = df_monitoring[
            df_monitoring["monitor_id"] == "al022024_obsv_2024-07-04T15:00:00"
        ].copy()
        df_monitoring_test[
            [
                "monitor_id",
                "name",
                "atcf_id",
                "obsv_trigger",
            ]
        ] = (
            "TEST_MONITOR_ID",
            "TEST_STORM_NAME",
            "TEST_ATCF_ID",
            True,
        )
        df_monitoring = pd.concat(
            [df_monitoring, df_monitoring_test], ignore_index=True
        )
    return df_monitoring


def get_distribution_list() -> pd.DataFrame:
    """Load distribution list from blob storage."""
    if TEST_LIST:
        blob_name = f"{blob.PROJECT_PREFIX}/email/test_distribution_list.csv"
    else:
        blob_name = f"{blob.PROJECT_PREFIX}/email/distribution_list.csv"
    return blob.load_csv_from_blob(blob_name)


def load_email_record() -> pd.DataFrame:
    """Load record of emails that have been sent."""
    blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
    return blob.load_csv_from_blob(blob_name)


def update_obsv_trigger_emails():
    df_monitoring = monitoring_utils.load_existing_monitoring_points("obsv")
    df_existing_email_record = load_email_record()
    if TEST_STORM:
        df_monitoring = add_test_row_to_monitoring(df_monitoring, "obsv")
        df_existing_email_record = df_existing_email_record[
            ~(
                (df_existing_email_record["atcf_id"] == "TEST_ATCF_ID")
                & (df_existing_email_record["email_type"] == "obsv")
            )
        ]
    dicts = []
    for atcf_id, group in df_monitoring.groupby("atcf_id"):
        if (
            atcf_id
            in df_existing_email_record[
                df_existing_email_record["email_type"] == "obsv"
            ]["atcf_id"].unique()
        ):
            print(f"already sent obsv email for {atcf_id}")
        else:
            for monitor_id, row in group.set_index("monitor_id").iterrows():
                if row["obsv_trigger"]:
                    try:
                        print(f"sending obsv email for {monitor_id}")
                        send_trigger_email(
                            monitor_id=monitor_id, trigger_name="obsv"
                        )
                        dicts.append(
                            {
                                "monitor_id": monitor_id,
                                "atcf_id": atcf_id,
                                "email_type": "obsv",
                            }
                        )
                    except Exception as e:
                        print(f"could not send email for {monitor_id}: {e}")

    df_new_email_record = pd.DataFrame(dicts)
    df_combined_email_record = pd.concat(
        [df_existing_email_record, df_new_email_record], ignore_index=True
    )
    blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
    blob.upload_csv_to_blob(blob_name, df_combined_email_record)


def update_fcast_trigger_emails():
    """Cycle through all historical monitoring points to see if we should have
    sent a trigger email for any of them. If we need to send any emails,
    send them.
    """
    df_monitoring = monitoring_utils.load_existing_monitoring_points("fcast")
    df_existing_email_record = load_email_record()
    if TEST_STORM:
        df_monitoring = add_test_row_to_monitoring(df_monitoring, "fcast")
        df_existing_email_record = df_existing_email_record[
            ~(
                (df_existing_email_record["atcf_id"] != "TEST_ATCF_ID")
                & (
                    df_existing_email_record["email_type"].isin(
                        ["readiness", "action"]
                    )
                )
            )
        ]
    dicts = []
    for atcf_id, group in df_monitoring.groupby("atcf_id"):
        for trigger_name in ["readiness", "action"]:
            if (
                atcf_id
                in df_existing_email_record[
                    df_existing_email_record["email_type"] == trigger_name
                ]["atcf_id"].unique()
            ):
                print(f"already sent {trigger_name} email for {atcf_id}")
            else:
                for (
                    monitor_id,
                    row,
                ) in group.set_index("monitor_id").iterrows():
                    if (
                        row[f"{trigger_name}_trigger"]
                        and not row["past_cutoff"]
                    ):
                        try:
                            print(
                                f"sending {trigger_name} email for "
                                f"{monitor_id}"
                            )
                            send_trigger_email(
                                monitor_id=monitor_id,
                                trigger_name=trigger_name,
                            )
                            dicts.append(
                                {
                                    "monitor_id": monitor_id,
                                    "atcf_id": atcf_id,
                                    "email_type": trigger_name,
                                }
                            )
                        except Exception as e:
                            print(
                                f"could not send email for {monitor_id}: {e}"
                            )

    df_new_email_record = pd.DataFrame(dicts)
    df_combined_email_record = pd.concat(
        [df_existing_email_record, df_new_email_record], ignore_index=True
    )
    blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
    blob.upload_csv_to_blob(blob_name, df_combined_email_record)


def send_info_emails(fcast_obsv: Literal["fcast", "obsv"]):
    pass


def send_trigger_email(monitor_id: str, trigger_name: str):
    """Send trigger email to distribution list."""
    fcast_obsv = "fcast" if trigger_name in ["readiness", "action"] else "obsv"
    df_monitoring = monitoring_utils.load_existing_monitoring_points(
        fcast_obsv
    )
    if TEST_STORM:
        df_monitoring = add_test_row_to_monitoring(df_monitoring, fcast_obsv)
    monitoring_point = df_monitoring.set_index("monitor_id").loc[monitor_id]
    haiti_tz = pytz.timezone("America/Port-au-Prince")
    cyclone_name = monitoring_point["name"]
    issue_time = monitoring_point["issue_time"]
    issue_time_hti = issue_time.astimezone(haiti_tz)
    pub_time = issue_time_hti.strftime("%Hh%M")
    pub_date = issue_time_hti.strftime("%-d %b %Y")
    for en_mo, fr_mo in FRENCH_MONTHS.items():
        pub_date = pub_date.replace(en_mo, fr_mo)
    if trigger_name == "readiness":
        trigger_name_fr = "mobilisation"
    elif trigger_name == "action":
        trigger_name_fr = "action"
    else:
        trigger_name_fr = "observationnel"
    fcast_obsv_fr = "observation" if fcast_obsv == "obsv" else "prévision"

    distribution_list = get_distribution_list()
    to_list = distribution_list[distribution_list["trigger"] == "to"]
    cc_list = distribution_list[distribution_list["trigger"] == "cc"]

    test_subject = "TEST : " if TEST_STORM else ""

    environment = Environment(loader=FileSystemLoader(TEMPLATES_DIR))

    template_name = "observational" if trigger_name == "obsv" else trigger_name
    template = environment.get_template(f"{template_name}.html")
    msg = EmailMessage()
    msg.set_charset("utf-8")
    msg["Subject"] = (
        f"{test_subject}Action anticipatoire Haïti – "
        f"déclencheur {trigger_name_fr} atteint pour "
        f"{cyclone_name}"
    )
    msg["From"] = Address(
        "Centre de données humanitaires OCHA",
        EMAIL_ADDRESS.split("@")[0],
        EMAIL_ADDRESS.split("@")[1],
    )
    msg["To"] = [
        Address(
            row["name"],
            row["email"].split("@")[0],
            row["email"].split("@")[1],
        )
        for _, row in to_list.iterrows()
    ]
    msg["Cc"] = [
        Address(
            row["name"],
            row["email"].split("@")[0],
            row["email"].split("@")[1],
        )
        for _, row in cc_list.iterrows()
    ]
    chd_banner_cid = make_msgid(domain="humdata.org")
    ocha_logo_cid = make_msgid(domain="humdata.org")

    html_str = template.render(
        name=cyclone_name,
        pub_time=pub_time,
        pub_date=pub_date,
        fcast_obsv=fcast_obsv_fr,
        test_email=TEST_STORM,
        chd_banner_cid=chd_banner_cid[1:-1],
        ocha_logo_cid=ocha_logo_cid[1:-1],
    )
    text_str = html2text(html_str)
    msg.set_content(text_str)
    msg.add_alternative(html_str, subtype="html")

    for filename, cid in zip(
        ["centre_banner.png", "ocha_logo_wide.png"],
        [chd_banner_cid, ocha_logo_cid],
    ):
        img_path = STATIC_DIR / filename
        with open(img_path, "rb") as img:
            msg.get_payload()[1].add_related(
                img.read(), "image", "png", cid=cid
            )

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, context=context) as server:
        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.sendmail(
            EMAIL_ADDRESS,
            to_list["email"].tolist() + cc_list["email"].tolist(),
            msg.as_string(),
        )
