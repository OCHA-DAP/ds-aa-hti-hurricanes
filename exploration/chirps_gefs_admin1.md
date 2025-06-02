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

# CHIRPS-GEFS admin1

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import xarray as xr

from src.datasources import codab, chirps_gefs
from src.utils import raster
```

```python
adm1 = codab.load_codab_from_blob(admin_level=1)
```

```python
adm1.plot()
```

```python
issue_date = pd.Timestamp("2024-07-03")
second_valid_date = pd.Timestamp("2024-07-04")

dss = []
for valid_date in valid_dates:
    dss.append(chirps_gefs.load_chirps_gefs_raster(issue_date, valid_date))

ds = dss[0] + dss[1]
```

```python
ds
```

```python
ds.rio.clip(adm1.geometry).plot()
```

```python
ds_up = raster.upsample_dataarray(
    ds, resolution=0.01, lat_dim="y", lon_dim="x"
)
```

```python
dicts = []
for pcode, row in adm1.groupby("ADM1_PCODE"):
    ds_clip = ds_up.rio.clip(row.geometry)
    dicts.append(
        {
            "ADM1_PCODE": pcode,
            "ADM1_FR": row.iloc[0]["ADM1_FR"],
            "issue_date": issue_date,
            "second_valid_date": second_valid_date,
            "roll2_mean": ds_clip.mean().values,
        }
    )
df = pd.DataFrame(dicts)
df
```
