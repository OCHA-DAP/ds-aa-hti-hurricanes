import datetime
import os
import platform
import shutil
from pathlib import Path
from subprocess import Popen
from typing import Literal

import pandas as pd
import requests
import xarray as xr

from src.utils import blob

IMERG_ZARR_ROOT = "az://global/imerg.zarr"

IMERG_BASE_URL = (
    "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGD"
    "{run}.06/{date:%Y}/{date:%m}/3B-DAY-{run}.MS.MRG.3IMERG."
    "{date:%Y%m%d}-S000000-E235959.V06.nc4"
)


def download_imerg(
    date: datetime.datetime,
    run: Literal["E", "L"] = "L",
    save_path: str = Path("temp/imerg_temp.nc"),
    verbose: bool = False,
):
    url = IMERG_BASE_URL.format(run=run, date=date)
    if verbose:
        print("downloading from " + url)
    result = requests.get(url)
    try:
        result.raise_for_status()
        if os.path.exists(save_path):
            os.remove(save_path)
        f = open(save_path, "wb")
        f.write(result.content)
        f.close()
        if verbose:
            print("contents of URL written to " + save_path)
    except requests.exceptions.HTTPError as err:
        print(err)
        print("failed to download from " + url)


def process_imerg(path: str = "temp/imerg_temp.nc"):
    ds = xr.open_dataset(path)
    ds = ds.transpose("lat", "lon", "time", "nv")
    da = ds["precipitationCal"]
    da["time"] = pd.to_datetime(
        [pd.Timestamp(t.strftime("%Y-%m-%d")) for t in da["time"].values]
    )
    return da


def load_imerg_zarr():
    fs = blob.get_fs()
    return xr.open_zarr(fs.get_mapper(IMERG_ZARR_ROOT), consolidated=True)


def append_imerg_zarr(da):
    fs = blob.get_fs()
    da.to_zarr(
        fs.get_mapper(IMERG_ZARR_ROOT),
        mode="a",
        append_dim="time",
        consolidated=True,
    )


def create_auth_files():
    # script to set credentials from
    # https://disc.gsfc.nasa.gov/information/howto?title=How%20to%20Generate%20Earthdata%20Prerequisite%20Files
    IMERG_USERNAME = os.environ["IMERG_USERNAME"]
    IMERG_PASSWORD = os.environ["IMERG_PASSWORD"]

    urs = "urs.earthdata.nasa.gov"  # Earthdata URL to call for authentication

    homeDir = os.path.expanduser("~") + os.sep

    with open(homeDir + ".netrc", "w") as file:
        file.write(
            "machine {} login {} password {}".format(
                urs, IMERG_USERNAME, IMERG_PASSWORD
            )
        )
        file.close()
    with open(homeDir + ".urs_cookies", "w") as file:
        file.write("")
        file.close()
    with open(homeDir + ".dodsrc", "w") as file:
        file.write("HTTP.COOKIEJAR={}.urs_cookies\n".format(homeDir))
        file.write("HTTP.NETRC={}.netrc".format(homeDir))
        file.close()

    print("Saved .netrc, .urs_cookies, and .dodsrc to:", homeDir)

    # Set appropriate permissions for Linux/macOS
    if platform.system() != "Windows":
        Popen("chmod og-rw ~/.netrc", shell=True)
    else:
        # Copy dodsrc to working directory in Windows
        shutil.copy2(homeDir + ".dodsrc", os.getcwd())
        print("Copied .dodsrc to:", os.getcwd())
