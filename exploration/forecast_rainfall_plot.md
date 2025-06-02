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

# Forecast rainfall plot

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from src.datasources import chirps_gefs, codab
```

```python
adm1 = codab.load_codab_from_blob(admin_level=1)
```

```python
adm1.plot()
```

```python
issue_date = pd.Timestamp("2024-07-03")
valid_dates = [pd.Timestamp("2024-07-03"), pd.Timestamp("2024-07-04")]

dss = []
for valid_date in valid_dates:
    dss.append(chirps_gefs.load_chirps_gefs_raster(issue_date, valid_date))

ds = dss[0] + dss[1]
```

```python
fig, ax = plt.subplots(dpi=300)
adm1.boundary.plot(ax=ax)
ds.plot(ax=ax)
```

```python
fig, ax = plt.subplots(dpi=300)
adm1.boundary.plot(ax=ax)
dss[0].plot(ax=ax)
```
