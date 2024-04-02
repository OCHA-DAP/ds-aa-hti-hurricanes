import os
from io import StringIO

import pandas as pd
from azure.storage.blob import ContainerClient
from dotenv import load_dotenv

load_dotenv()

PROD_BLOB_SAS = os.getenv("PROD_BLOB_SAS")
PROD_BLOB_BASE_URL = "https://imb0chd0prod.blob.core.windows.net/"
AA_BLOB_BASE_URL = PROD_BLOB_BASE_URL + "aa-data"
AA_BLOB_URL = AA_BLOB_BASE_URL + "?" + PROD_BLOB_SAS


container_client = ContainerClient.from_container_url(AA_BLOB_URL)


def load_blob_data(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return data


def upload_blob_data(blob_name, data):
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data, overwrite=True)


def list_container_blobs(name_starts_with=None):
    return [
        blob.name
        for blob in container_client.list_blobs(
            name_starts_with=name_starts_with
        )
    ]
