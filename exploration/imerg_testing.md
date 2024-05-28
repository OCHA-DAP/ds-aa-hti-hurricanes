---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: ds-aa-hti-hurricanes
    language: python
    name: ds-aa-hti-hurricanes
---

# IMERG testing

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import requests
from requests.auth import HTTPBasicAuth

# URL of the file to download
url = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDL.06/2024/05/3B-DAY-L.MS.MRG.3IMERG.20240527-S000000-E235959.V06.nc4"
subset = "?lat[0:10],lon[0:10]"

# Your credentials
username = "your_username"
password = "your_password"

# Send the request with Basic Authentication
result = requests.get(url + subset)
```

```python
FILENAME = "temp/imerg_test"
try:
    result.raise_for_status()
    f = open(FILENAME, "wb")
    f.write(result.content)
    f.close()
    print("contents of URL written to " + FILENAME)
except:
    print("requests.get() returned an error code " + str(result.status_code))
```

```python
try:
    result.raise_for_status()
except:
    print("DFAS")
```

```python
import xarray as xr

ds = xr.load_dataset(FILENAME)
```

```python
ds.isel(time=0)["precipitationCal"].plot()
```

```python
ds
```

```python
import xarray as xr
```

```python
from subprocess import Popen
from getpass import getpass
import platform
import os
import shutil

urs = "urs.earthdata.nasa.gov"  # Earthdata URL to call for authentication
prompts = [
    "Enter NASA Earthdata Login Username \n"
        "(or create an account at urs.earthdata.nasa.gov): ",
    "Enter NASA Earthdata Login Password: ",
]

homeDir = os.path.expanduser("~") + os.sep

with open(homeDir + ".netrc", "w") as file:
    file.write(
        "machine {} login {} password {}".format(
            urs, getpass(prompt=prompts[0]), getpass(prompt=prompts[1])
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
```

```python
URL = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDL.06/2024/05/3B-DAY-L.MS.MRG.3IMERG.20240527-S000000-E235959.V06.nc4"
SAMPLE_URL = (
    "https://data.gesdisc.earthdata.nasa.gov/data/MERRA2/path/to/granule.nc4"
)
```

```python
import xarray as xr
ds = xr.open_dataset(SAMPLE_URL)
```

```python
import netCDF4 as nc4

nc = nc4.Dataset('https://data.gesdisc.earthdata.nasa.gov/data/MERRA2/path/to/granule.nc4')
```

```python

```

```python
import requests

# Set the URL string to point to a specific data URL. Some generic examples are:
#   https://data.gesdisc.earthdata.nasa.gov/data/MERRA2/path/to/granule.nc4

# URL = 'your_URL_string_goes_here'

# Set the FILENAME string to the data file name,
# the LABEL keyword value, or any customized name.
FILENAME = 'temp/imerg_test'

import requests
result = requests.get(SAMPLE_URL)
try:
    result.raise_for_status()
    f = open(FILENAME,'wb')
    f.write(result.content)
    f.close()
    print('contents of URL written to '+FILENAME)
except:
    print('requests.get() returned an error code '+str(result.status_code))
```

```python
from pydap.client import open_url
from pydap.cas.urs import setup_session
from getpass import getpass

dataset_url = 'https://servername/opendap/path/file[.format[?subset]]'

username = 'tristandowning1'
password = getpass()

try:
    session = setup_session(username, password, check_url=dataset_url)
    dataset = open_url(dataset_url, session=session)
except AttributeError as e:
    print('Error:', e)
    print('Please verify that the dataset URL points to an OPeNDAP server, '
          'the OPeNDAP server is accessible, or '
          'that your username and password are correct.')
```

```python
import ssl
print(ssl.OPENSSL_VERSION)
```
