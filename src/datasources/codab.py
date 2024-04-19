import os
import shutil
from pathlib import Path

import geopandas as gpd
import requests

DATA_DIR = Path(os.environ["AA_DATA_DIR_NEW"])
CODAB_RAW_DIR = DATA_DIR / "public" / "raw" / "hti" / "codab"


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
