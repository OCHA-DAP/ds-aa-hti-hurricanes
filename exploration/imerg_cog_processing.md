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

from src.datasources import imerg, codab, chirps
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
# v7 current
blob_names = blob.list_container_blobs(
    name_starts_with="imerg/v7/imerg-daily-late-2024", container_name="global"
)
```

```python
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
```

```python
df = pd.DataFrame(dicts)
```

```python
df
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/imerg/hti_imerg_daily_mean_v7_2024.parquet"
blob.upload_parquet_to_blob(blob_name, df)
```

```python
# v6 historical
```

```python
blob_names = blob.list_container_blobs(
    name_starts_with="imerg/v6/", container_name="global"
)
```

```python
blob_names[-1]
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
test = imerg.load_imerg_mean()
test
```

```python
blob_names = blob.list_container_blobs(
    name_starts_with="imerg/v7/", container_name="global"
)
```

```python
cog_url = (
    f"https://{blob.DEV_BLOB_NAME}.blob.core.windows.net/global/"
    f"{blob_names[-1]}?{blob.DEV_BLOB_SAS}"
)
```

```python
blob_names[-1]
```

```python
da_in = rxr.open_rasterio(
    cog_url, masked=True, chunks={"band": 1, "x": 3600, "y": 1800}
)
```

```python
da_in
```
