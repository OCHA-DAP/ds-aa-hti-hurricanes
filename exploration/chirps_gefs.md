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
adm0 = codab.load_codab(admin_level=0)
adm0.plot()
```

```python
start_date = "2000-01-01"
end_date = "2023-12-31"

issue_date_range = pd.date_range(start=start_date, end=end_date, freq="D")

verbose = False

dfs = []
for issue_date in tqdm(issue_date_range):
    das_i = []
    for leadtime in range(16):
        valid_date = issue_date + pd.Timedelta(days=leadtime)
        try:
            da_in = chirps_gefs.load_chirps_gefs_raster(issue_date, valid_date)
            da_in["valid_date"] = valid_date
            das_i.append(da_in)
        except ResourceNotFoundError as e:
            if verbose:
                print(f"{e} for {issue_date} {valid_date}")

    if das_i:
        da_i = xr.concat(das_i, dim="valid_date")
        da_i_clip = da_i.rio.clip(adm0.geometry, all_touched=True)
        df_in = (
            da_i_clip.mean(dim=["x", "y"])
            .to_dataframe(name="mean")["mean"]
            .reset_index()
        )
        df_in["issue_date"] = issue_date
        dfs.append(df_in)
    else:
        if verbose:
            print(f"no files for issue_date {issue_date}")
```

```python
df = pd.concat(dfs, ignore_index=True)
```

```python
data = df.to_csv(index=False)
```

```python
blob_proc_dir = "processed/chirps/gefs/hti/"
blob_name = "hti_chirps_gefs_mean_daily_2000_2023.csv"
blob.upload_blob_data(blob_proc_dir + blob_name, data)
```

```python
df
```

```python
df.loc[df["mean"].idxmax()]
```

```python
df[df["valid_date"] == "2016-10-04"].plot(x="issue_date", y="mean")
```
