---
jupyter:
  jupytext:
    formats: md,ipynb
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

# IMERG COG processing

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import time
import datetime
from io import BytesIO

import pandas as pd
import matplotlib.pyplot as plt
import rioxarray as rxr
import xarray as xr
from tqdm.notebook import tqdm

from src.datasources import imerg, codab
from src.utils import blob, raster
```

```python
adm0 = codab.load_codab_from_blob()
```

```python
minx, miny, maxx, maxy = adm0.total_bounds
```

```python
minx, miny, maxx, maxy
```

```python
blob_names = existing_files = [
    x.name
    for x in blob.dev_glb_container_client.list_blobs(
        name_starts_with="imerg/v6/"
    )
]
```

```python
# do everything in one step

dicts = []
for blob_name in tqdm(blob_names):
    cog_url = (
        f"https://{blob.DEV_BLOB_NAME}.blob.core.windows.net/global/"
        f"{blob_name}?{blob.DEV_BLOB_SAS}"
    )
    da_in = rxr.open_rasterio(
        cog_url, masked=True, chunks={"band": 1, "x": 3600, "y": 1800}
    )
    da_in = da_in.squeeze(drop=True)
    date_in = pd.to_datetime(blob_name.split(".")[0][-10:])
    da_box = da_in.sel(x=slice(minx, maxx), y=slice(miny, maxy))
    da_box_up = raster.upsample_dataarray(
        da_box, lat_dim="y", lon_dim="x", resolution=0.05
    )
    da_box_up = da_box_up.rio.write_crs(4326)
    da_clip = da_box_up.rio.clip(adm0.geometry, all_touched=True)
    da_mean = da_clip.mean()
    mean_val = float(da_mean.compute())
    dicts.append({"date": date_in, "mean": mean_val})
    df_in = pd.DataFrame([{"date": date_in, "mean": mean_val}])
    blob_name = f"{blob.PROJECT_PREFIX}/processed/imerg/date_means/imerg-late-hti-mean_{date_in.date()}.parquet"
    blob.upload_parquet_to_blob(blob_name, df_in)
```

```python
# load all the individual parquets
dfs = []
for blob_name in tqdm(blob_names):
    date_in = pd.to_datetime(blob_name.split(".")[0][-10:])
    parquet_name = f"{blob.PROJECT_PREFIX}/processed/imerg/date_means/imerg-late-hti-mean_{date_in.date()}.parquet"
    df_in = blob.load_parquet_from_blob(parquet_name)
    dfs.append(df_in)
```

```python
df = pd.concat(dfs)
```

```python
df
```

```python
blob_name = (
    f"{blob.PROJECT_PREFIX}/processed/imerg/hti_imerg_daily_mean_v6.parquet"
)
blob.upload_parquet_to_blob(blob_name, df)
```

```python
test = blob.load_parquet_from_blob(blob_name)
test
```
