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

# Historical forecasts

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import os
import gzip
from ftplib import FTP
from pathlib import Path
from io import StringIO, BytesIO
from datetime import datetime

import fiona
import requests
import geopandas as gpd
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm

from src.utils import blob
from src.datasources import nhc
```

```python
# nhc.download_historical_forecasts()
```

```python
nhc.process_historical_forecasts()
```

```python
nhc.calculate_hti_distance()
```

```python
NHC_RAW_BLOB = "raw/noaa/nhc/historical_forecasts/{year}/a{atcf_id}.csv"
```

```python
MATTHEW = "AL142016"
MATTHEW_YEAR = int(MATTHEW[-4:])
MATTHEW_NUM = int(MATTHEW[2:4])
# MATTHEW_BLOB = NHC_RAW_BLOB.format(year=MATTHEW_YEAR, num=MATTHEW_NUM)
MATTHEW_ATCF_ID = f"al{MATTHEW_NUM}{MATTHEW_YEAR}"
```

```python
NHC_RAW_BLOB.format(year=MATTHEW_YEAR, atcf_id=MATTHEW_ATCF_ID)
```

```python
blob_names = blob.list_container_blobs(
    name_starts_with="raw/noaa/nhc/historical_forecasts/"
)
blob_names = [x for x in blob_names if x.endswith(".csv")]
```

```python
def proc_latlon(l):
    c = l[-1]
    if c in ["N", "E"]:
        return float(l[:-1]) / 10
    elif c in ["S", "W"]:
        return -float(l[:-1]) / 10


dfs = []
for blob_name in tqdm(blob_names):
    df_in = pd.read_csv(BytesIO(blob.load_blob_data(blob_name)))

    atcf_id = blob_name.removesuffix(".csv")[-8:]

    cols = ["YYYYMMDDHH", "TAU", "LatN/S", "LonE/W", "MSLP", "VMAX"]
    dff = df_in[df_in["TECH"] == " OFCL"][cols]
    if dff.empty:
        continue

    dff["issue_time"] = dff["YYYYMMDDHH"].apply(
        lambda x: datetime.strptime(str(x), "%Y%m%d%H")
    )
    dff["valid_time"] = dff.apply(
        lambda row: row["issue_time"] + pd.Timedelta(hours=row["TAU"]), axis=1
    )

    dff["lat"] = dff["LatN/S"].apply(proc_latlon)
    dff["lon"] = dff["LonE/W"].apply(proc_latlon)
    dff = dff.rename(
        columns={"TAU": "leadtime", "MSLP": "pressure", "VMAX": "windspeed"}
    )
    cols = ["issue_time", "valid_time", "lat", "lon", "windspeed", "pressure"]
    dff = dff[cols]
    dff = dff.loc[~dff.duplicated()]
    # dff["name"] = stormname
    dff["atcf_id"] = atcf_id
    dfs.append(dff)
```

```python
df = pd.concat(dfs, ignore_index=True)
```

```python
df
```

```python
save_blob = "processed/noaa/nhc/historical_forecasts/al_2000_2022.csv"
blob.upload_blob_data(save_blob, df.to_csv(index=False))
```

```python
test = pd.read_csv(
    BytesIO(blob.load_blob_data(save_blob)),
    parse_dates=["issue_time", "valid_time"],
)
```

```python
gdf = test[test["atcf_id"] == MATTHEW_ATCF_ID]
```

```python

```
