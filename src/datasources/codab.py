import requests

from src.utils import blob


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
