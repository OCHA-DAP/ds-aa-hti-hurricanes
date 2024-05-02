import os
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd
import xarray as xr
from shapely import Point

from src.constants import HTI_ASAP0_ID

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
IBTRACS_RAW_DIR = DATA_DIR / "public" / "raw" / "glb" / "ibtracs"
IBTRACS_PROC_DIR = DATA_DIR / "public" / "processed" / "glb" / "ibtracs"
IBTRACS_HTI_PROC_DIR = DATA_DIR / "public" / "processed" / "hti" / "ibtracs"

CERF_SIDS = [
    "2016273N13300",  # Matthew
    "2008245N17323",  # Ike
    "2008238N13293",  # Gustav
    "2008241N19303",  # Hanna
    "2008229N18293",  # Fay
    "2012296N14283",  # Sandy
]
IMPACT_SIDS = [
    "2004258N16300",  # Jeanne
    "2007297N18300",  # Noel
    "2020233N14313",  # Laura
]


def load_all_adm0_distances(
    wind_provider: Literal["usa", "wmo"] = "usa",
    start_year: int = 2000,
    end_year: int = 2023,
):
    return pd.read_parquet(
        IBTRACS_PROC_DIR
        / f"all_adm0_distances_{wind_provider}_{start_year}-{end_year}.parquet"
    )


def load_ibtracs_with_wind(wind_provider: Literal["usa", "wmo"] = "usa"):
    """Load IBTrACS data with wind speed data from a specific provider."""
    load_path = IBTRACS_PROC_DIR / f"ibtracs_with_{wind_provider}_wind.parquet"
    df = pd.read_parquet(load_path)
    geometry = [Point(lon, lat) for lon, lat in zip(df["lon"], df["lat"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    return gdf


def process_hti_distances():
    all_distances = load_all_adm0_distances()
    all_tracks = load_ibtracs_with_wind()
    hti_distances = all_distances[all_distances["asap0_id"] == HTI_ASAP0_ID]
    hti_distances_names = hti_distances.merge(
        all_tracks, on="row_id", how="left"
    )

    if not IBTRACS_HTI_PROC_DIR.exists():
        os.makedirs(IBTRACS_HTI_PROC_DIR, exist_ok=True)
    filename = "hti_distances.csv"
    hti_distances_names.drop(
        columns=["row_id", "geometry", "asap0_id"]
    ).to_csv(IBTRACS_HTI_PROC_DIR / filename, index=False)


def load_hti_distances():
    return pd.read_csv(
        IBTRACS_HTI_PROC_DIR / "hti_distances.csv", parse_dates=["time"]
    )


def load_raw_ibtracs():
    filename = "IBTrACS.ALL.v04r00.nc"
    return xr.open_dataset(IBTRACS_RAW_DIR / filename)


def process_ibtracs_sid_atcf_names():
    ds = load_raw_ibtracs()
    ds_f = ds[["sid", "usa_atcf_id", "name"]]
    df = ds_f.to_dataframe()
    df_match = df.reset_index()[["sid", "usa_atcf_id", "name"]]
    dff = df_match[df_match["usa_atcf_id"] != b""]
    for x in dff.columns:
        dff.loc[:, x] = dff[x].astype(str)
    dff = dff[~dff.duplicated()]
    filename = "sid_atcf_name.csv"
    dff.to_csv(IBTRACS_PROC_DIR / filename, index=False)


def load_ibtracs_sid_atcf_names():
    return pd.read_csv(IBTRACS_PROC_DIR / "sid_atcf_name.csv")
