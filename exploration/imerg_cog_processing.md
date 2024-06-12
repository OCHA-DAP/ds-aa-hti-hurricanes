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
from io import BytesIO

import pandas as pd
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
das = []
for date in tqdm(pd.date_range("2007-07-01", "2007-10-01")):
    da_in = imerg.open_imerg_raster(date)
    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date
    das.append(da_in)
```

```python
das[0]
```

```python
da_glb = xr.concat(das, dim="date")
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
```

```python
da_clip = da_box_up.rio.clip(adm0.geometry, all_touched=True)
```

```python
da_clip.mean(dim=["x", "y"]).compute()
```

```python
da_clip.isel(date=0).plot()
```

```python
start = time.time()
test = imerg.open_imerg_raster(pd.Timestamp("2007-11-01"))
subset = test.sel(x=slice(minx, maxx), y=slice(miny, maxy))
print(time.time() - start)
start = time.time()
subset.plot()
print(time.time() - start)
start = time.time()
```

```python
da_up = raster.upsample_dataarray(
    subset, lat_dim="y", lon_dim="x", resolution=0.05
)
da_up = da_up.rio.write_crs(4326)
da_clip = da_up.rio.clip(adm0.geometry, all_touched=True)
```

```python
da_clip.where(da_clip > 0).plot()
```

```python
start = time.time()
input_path = "imerg/v07b/imerg-daily-late-2003-05-17.tif"
data = (
    blob.dev_glb_container_client.get_blob_client(input_path)
    .download_blob()
    .readall()
)
test = rxr.open_rasterio(BytesIO(data))
subset = test.sel(x=slice(minx, maxx), y=slice(miny, maxy))
print(time.time() - start)
start = time.time()
subset.plot()
print(time.time() - start)
start = time.time()
```
