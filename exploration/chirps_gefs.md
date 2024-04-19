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

# CHIRPS GEFS

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import xarray as xr
import rioxarray as rxr
import pandas as pd
from tqdm.notebook import tqdm

from src.datasources import codab, chirps_gefs
from src.utils import blob
```

```python
CHIRPS_GEFS_URL = "https://data.chc.ucsb.edu/products/EWX/data/forecasts/CHIRPS-GEFS_precip_v12/daily_16day/2000/{iss_year}/{iss_month:02d}/{iss_day:02d}/"
```

```python
adm0 = codab.load_codab(admin_level=0)
adm0.plot()
```

```python
total_bounds = adm0.total_bounds
```

```python
start_date = "2000-01-01"
end_date = "2023-12-31"

issue_date_range = pd.date_range(start=start_date, end=end_date, freq="D")
```

```python
existing_files = blob.list_container_blobs(name_starts_with=chirps_gefs.CHIRPS_GEFS_BLOB_DIR)
for issue_date in tqdm(issue_date_range):
    for leadtime in range(16):
        valid_date = issue_date + pd.Timedelta(days=leadtime)
        output_filename = f"chirps-gefs-hti_issued-{issue_date.date()}_valid-{valid_date.date()}.tif"
        chirps_gefs.download_chirps_gefs(
            issue_date, valid_date, total_bounds, existing_files, clobber=False
        )
```

```python
valid_date
```

```python
data = chirps_gefs.load_chirps_gefs_raster(
    issue_date, valid_date - pd.Timedelta(days=1)
)
```

```python
da = rxr.open_rasterio(data)
```

```python
da.plot()
```

```python
ax = adm0.boundary.plot()
da.plot(ax=ax)
```

```python

```

```python
ax = adm0.boundary.plot()
da_aoi.plot(ax=ax)
```
