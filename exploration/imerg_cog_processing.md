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
df_in
```

```python
f"imerg-late-hti-mean_{date_in.date()}.parquet"
```

```python
blob_names = existing_files = [
    x.name
    for x in blob.dev_glb_container_client.list_blobs(
        name_starts_with="imerg/v6/"
    )
]

das = []
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
    da_in["date"] = date_in
    das.append(da_in)

da_glb = xr.concat(das, dim="date")
```

```python
da_glb = xr.concat(das, dim="date")
```

```python
da_glb
```

```python
da_box = da_glb.sel(x=slice(minx, maxx), y=slice(miny, maxy))
```

```python
da_box_up = raster.upsample_dataarray(
    da_box, lat_dim="y", lon_dim="x", resolution=0.05
)
```

```python
da_box
```

```python
da_box_up = da_box_up.rio.write_crs(4326)
da_clip = da_box_up.rio.clip(adm0.geometry, all_touched=True)
```

```python
da_clip
```

```python
da_clip["date"] = pd.to_datetime(da_clip.date)
```

```python
da_clip
```

```python
%time da_mean = da_clip.mean(dim=["x", "y"])
```

```python
%time da_mean.isel(date=4).compute()
```

```python
2 * 7754 / 60 / 60
```

```python
f"{datetime.datetime.today() - pd.DateOffset(days=1):%Y%m%d}"
```

```python
f"{(datetime.datetime.today() - pd.DateOffset(days=1)).date()}"
```

```python
fig, ax = plt.subplots(dpi=300)
adm0.boundary.plot(ax=ax, linewidth=0.5, color="white")
da_clip.sel(date="2007-10-28").plot(ax=ax)
```
