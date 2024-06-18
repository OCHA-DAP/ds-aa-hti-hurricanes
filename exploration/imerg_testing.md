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
import os

import dask
from dask.distributed import Client
import dask.array as da
import pandas as pd
import zarr
import fsspec
import xarray as xr
import time
import matplotlib.pyplot as plt
from tqdm.notebook import tqdm

from src.datasources import imerg, codab
from src.utils import blob
```

Just loading CODAB for later plotting

```python
adm0 = codab.load_codab_from_blob(admin_level=0)
```

Create auth files to access NASA GES DISC server

```python
# imerg.create_auth_files()
```

Cycle over all days to:

1. Download IMERG file as entire file (streaming from the `.nc` wasn't working,
I think due to permissions)
2. Process IMERG (basically just selecting a single variable)
3. Append to `zarr` store in the blob -
this is what was taking so much time, and getting slower.

Doing the same things in Databricks was a bit quicker for some steps,
but the appending process still took a very long time.

Only thing I found is that using `fsspec` is a bit faster than using
`zarr.storage.ABSStore`

```python
ds = imerg.load_imerg_zarr()

for date in tqdm(pd.date_range("2003-03-11", "2020-01-19")):
    start = time.time()
    print(date)
    if date in ds.time:
        continue
    imerg.download_imerg(date)
    da_in = imerg.process_imerg()
    imerg.append_imerg_zarr(da_in)
    print(time.time() - start)
```

I tried monitoring some of the processes with Dask but was
hard to tell why things were so slow

```python
client = Client()
```

```python
print(client)
```

```python
start = time.time()
ds = imerg.load_imerg_zarr()
print(time.time() - start)
```

Tried saving to a `zarr` locally but took too long

```python
ds.to_zarr("temp/imerg.zarr")
```

Attempts to read from blob using `fsspec` instead.

```python
store = blob.get_fs().get_mapper(imerg.IMERG_ZARR_ROOT)
```

```python
start = time.time()
ds_nc = xr.open_zarr(store)
print(time.time() - start)
```

```python
root = zarr.open(store, mode="r")
```

```python
root["precipitationCal"]
```

```python
root["time"][:]
```

```python
root.tree()
```

```python
type(root)
```

```python
ds["precipitationCal"].isel(time=100).mean().compute()
```

```python
ds.time
```

```python
start = time.time()
ds.isel(time=100).mean().compute()
print(time.time() - start)
```

```python
chunk_sizes = {"lat": 20, "lon": 20, "time": 100}

rechunked_ds = ds.chunk(chunk_sizes)
```

```python
rechunked_ds.chunks
```

```python
ds
```

```python
minx, miny, maxx, maxy = adm0.total_bounds
```

```python
ds_box_re = rechunked_ds.sel(lon=slice(minx, maxx), lat=slice(miny, maxy))
```

```python
ds_box = ds.sel(lon=slice(minx, maxx), lat=slice(miny, maxy))
```

```python
minx, miny, maxx, maxy
```

```python
ds_box["precipitationCal"].mean(dim="time").compute()
```

```python
ds_box_re["precipitationCal"].mean(dim="time").compute()
```

```python
start = time.time()
ds_box["precipitationCal"].isel(time=-10).plot()
print(time.time() - start)
```

```python
start = time.time()
ds_box.mean().compute()
print(time.time() - start)
```

```python
ds_box["precipitationCal"].isel(time=10).values
```

```python
da_box = da_box.rio.write_crs(4326)
da_box = da_box.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
```

```python
da_clip = da_box.rio.clip(adm0.geometry)
```

```python
fig, ax = plt.subplots()
adm0.boundary.plot(ax=ax)
da_clip.isel(time=0).plot(ax=ax)
```

```python

```
