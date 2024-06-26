import os
import smtplib
import ssl
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path

from html2text import html2text
from jinja2 import Environment, FileSystemLoader

from src.utils import blob

EMAIL_HOST = os.getenv("CHD_DS_HOST")
EMAIL_PORT = int(os.getenv("CHD_DS_PORT"))
EMAIL_PASSWORD = os.getenv("CHD_DS_EMAIL_PASSWORD")
EMAIL_USERNAME = os.getenv("CHD_DS_EMAIL_USERNAME")
EMAIL_ADDRESS = os.getenv("CHD_DS_EMAIL_ADDRESS")
TEST_RUN = os.getenv("TEST_RUN")
if TEST_RUN is None:
    TEST_RUN = True
TEMPLATES_DIR = Path("src/email/templates")
STATIC_DIR = Path("src/email/static")


def get_distribution_list():
    if TEST_RUN:
        blob_name = f"{blob.PROJECT_PREFIX}/email/test_distribution_list.csv"
    else:
        blob_name = f"{blob.PROJECT_PREFIX}/email/distribution_list.csv"
    return blob.load_csv_from_blob(blob_name)


def send_trigger_emails():
    distribution_list = get_distribution_list()
    email_list = distribution_list[
        distribution_list["email_type"] == "trigger"
    ]
    to_list = email_list[email_list["to_cc"] == "to"]
    cc_list = email_list[email_list["to_cc"] == "cc"]

    test_subject = "TEST: " if TEST_RUN else ""
    cyclone_name = "TEST" if TEST_RUN else ""

    environment = Environment(loader=FileSystemLoader(TEMPLATES_DIR))

    for trigger_name in ["readiness", "action", "observational"]:
        print(trigger_name)
        template = environment.get_template(f"{trigger_name}.html")
        msg = EmailMessage()
        msg["Subject"] = (
            f"{test_subject}Action anticipatoire Haïti – "
            f"{trigger_name.capitalize()} déclencheur atteint pour "
            f"Cyclone {cyclone_name}"
        )
        msg["From"] = Address(
            "OCHA Centre for Humanitarian Data",
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
        print(msg["To"])
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
            pub_time="test",
            pub_date="test",
            test_email=TEST_RUN,
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

        with smtplib.SMTP_SSL(
            EMAIL_HOST, EMAIL_PORT, context=context
        ) as server:
            server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.sendmail(
                EMAIL_ADDRESS,
                to_list["email"].tolist() + cc_list["email"].tolist(),
                msg.as_string(),
            )
