import os
from typing import Literal

from azure.storage.blob import ContainerClient
from dotenv import load_dotenv

load_dotenv()

PROD_BLOB_SAS = os.getenv("PROD_BLOB_SAS")
PROD_BLOB_BASE_URL = "https://imb0chd0prod.blob.core.windows.net/"
PROD_BLOB_AA_BASE_URL = PROD_BLOB_BASE_URL + "aa-data"
PROD_BLOB_AA_URL = PROD_BLOB_AA_BASE_URL + "?" + PROD_BLOB_SAS

DEV_BLOB_SAS = os.getenv("DEV_BLOB_SAS")
DEV_BLOB_BASE_URL = "https://imb0chd0dev.blob.core.windows.net/"
DEV_BLOB_PROJ_BASE_URL = DEV_BLOB_BASE_URL + "projects"
DEV_BLOB_PROJ_URL = DEV_BLOB_PROJ_BASE_URL + "?" + DEV_BLOB_SAS


prod_container_client = ContainerClient.from_container_url(PROD_BLOB_AA_URL)
dev_container_client = ContainerClient.from_container_url(DEV_BLOB_PROJ_URL)


def load_blob_data(blob_name, prod_dev: Literal["prod", "dev"] = "prod"):
    if prod_dev == "dev":
        container_client = dev_container_client
    else:
        container_client = prod_container_client
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return data


def upload_blob_data(
    blob_name, data, prod_dev: Literal["prod", "dev"] = "prod"
):
    if prod_dev == "dev":
        container_client = dev_container_client
    else:
        container_client = prod_container_client
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data, overwrite=True)


def list_container_blobs(
    name_starts_with=None, prod_dev: Literal["prod", "dev"] = "prod"
):
    if prod_dev == "dev":
        container_client = dev_container_client
    else:
        container_client = prod_container_client
    return [
        blob.name
        for blob in container_client.list_blobs(
            name_starts_with=name_starts_with
        )
    ]
