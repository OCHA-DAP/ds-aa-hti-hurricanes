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
from azure.core.exceptions import ResourceNotFoundError

from src.datasources import codab, chirps_gefs
from src.utils import blob
```

```python
# chirps_gefs.process_chirps_gefs()
```

```python
adm0 = codab.load_codab(admin_level=0)
adm0.plot()
```

```python
df = chirps_gefs.load_chirps_gefs_mean_daily()
```

```python
df
```

```python
dates = pd.date_range("2000-01-01", "2024-01-01")
```

```python
missing_dates = []
for date in dates:
    if date not in df["issue_date"].unique():
        missing_dates.append(date)
```

```python
missing_dates
```
