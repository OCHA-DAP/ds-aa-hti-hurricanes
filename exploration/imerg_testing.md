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

```python
time.time()
```

```python
adm0 = codab.load_codab_from_blob(admin_level=0)
```

```python
# imerg.create_auth_files()
```

```python
ds = imerg.load_imerg_zarr()

for date in tqdm(pd.date_range("2000-06-01", "2020-01-19")):
    start = time.time()
    print(date)
    if date in ds.time:
        continue
    imerg.download_imerg(date)
    da_in = imerg.process_imerg()
    imerg.append_imerg_zarr(da_in)
    print(time.time() - start)
```

```python
fs = blob.get_fs()
da_in = imerg.process_imerg()
da_in.to_zarr(fs.get_mapper(imerg.IMERG_ZARR_ROOT), mode="w")
```

```python
imerg.download_imerg(d)
da_in = imerg.process_imerg()
```

```python
da = imerg.load_imerg_zarr()["precipitationCal"]
```

```python
ds = imerg.load_imerg_zarr()
```

```python
ds
```

```python
da = da.isel(time=slice(1, 2))
```

```python
da.to_zarr(fs.get_mapper(imerg.IMERG_ZARR_ROOT), mode="w")
```

```python
da.sortby("time")
```

```python
minx, miny, maxx, maxy = adm0.total_bounds
```

```python
da_box = da.sel(lon=slice(minx, maxx), lat=slice(miny, maxy))
```

```python
da_box
```

```python
fig, ax = plt.subplots()
adm0.boundary.plot(ax=ax)
da_box.isel(time=0).plot(ax=ax)
```

```python

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
