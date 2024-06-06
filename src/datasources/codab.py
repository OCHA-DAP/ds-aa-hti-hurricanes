import os
import shutil
from pathlib import Path

import geopandas as gpd
import requests

from src.utils import blob

DATA_DIR = Path(os.environ["AA_DATA_DIR_NEW"])
CODAB_RAW_DIR = DATA_DIR / "public" / "raw" / "hti" / "codab"


def process_buffer(distance_km: int = 230):
    adm0 = load_codab_from_blob(admin_level=0)
    buffer = adm0.to_crs(3857).buffer(distance=distance_km * 1000).to_crs(4326)
    blob.upload_gdf_to_blob(
        buffer,
        f"ds-aa-hti-hurricanes/processed/codab/"
        f"hti_buffer_{distance_km}km.shp.zip",
        prod_dev="dev",
    )


def load_buffer(distance_km: int = 230):
    buffer = blob.load_gdf_from_blob(
        f"ds-aa-hti-hurricanes/processed/codab/"
        f"hti_buffer_{distance_km}km.shp.zip",
        prod_dev="dev",
    )
    return buffer


def download_codab_to_blob():
    url = "https://data.fieldmaps.io/cod/originals/hti.shp.zip"
    # Download data from URL
    response = requests.get(url)
    response.raise_for_status()
    blob_name = "ds-aa-hti-hurricanes/raw/codab/hti.shp.zip"
    blob.upload_blob_data(blob_name, response.content, prod_dev="dev")


def load_codab_from_blob(admin_level: int = 0):
    shapefile = f"hti_adm{admin_level}.shp"
    gdf = blob.load_gdf_from_blob(
        "ds-aa-hti-hurricanes/raw/codab/hti.shp.zip",
        shapefile=shapefile,
        prod_dev="dev",
    )
    return gdf


def download_codab():
    url = "https://data.fieldmaps.io/cod/originals/hti.shp.zip"
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        if not CODAB_RAW_DIR.exists():
            os.makedirs(CODAB_RAW_DIR, exist_ok=True)
        with open(CODAB_RAW_DIR / "hti.shp.zip", "wb") as f:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, f)
    else:
        print(
            f"Failed to download file. "
            f"HTTP response code: {response.status_code}"
        )


def load_codab(admin_level: int = 0):
    gdf = gpd.read_file(CODAB_RAW_DIR / "hti.shp.zip")
    if admin_level == 2:
        cols = [x for x in gdf.columns if "ADM3" not in x]
        gdf = gdf.dissolve("ADM2_PCODE").reset_index()[cols]
    elif admin_level == 1:
        cols = [x for x in gdf.columns if "ADM3" not in x and "ADM2" not in x]
        gdf = gdf.dissolve("ADM1_PCODE").reset_index()[cols]
    elif admin_level == 0:
        cols = [
            x
            for x in gdf.columns
            if "ADM3" not in x and "ADM2" not in x and "ADM1" not in x
        ]
        gdf = gdf.dissolve("ADM0_PCODE").reset_index()[cols]
    return gdf
