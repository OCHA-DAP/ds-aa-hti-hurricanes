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

# Monitoring testing

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from src.datasources import nhc, codab, chirps_gefs
from src.utils import blob
```

```python
D_THRESH = 230
LT_CUTOFF_HRS = 36
```

```python
adm0 = codab.load_codab_from_blob().to_crs(3857)
```

```python
adm0.plot()
```

```python
df_gefs_all = chirps_gefs.load_recent_chirps_gefs_mean_daily()
gefs_most_recent_date = df_gefs_all["issue_date"].max()
df_gefs = df_gefs_all[df_gefs_all["issue_date"] == gefs_most_recent_date]
```

```python
df_gefs
```

```python
df_existing = nhc.load_recent_glb_forecasts()
df_existing = df_existing[df_existing["basin"] == "al"]
```

```python
df_existing
```

```python
threshs = {
    "readiness": {"p": 35, "s": 34, "lt_days": 5},
    "action": {"p": 42, "s": 64, "lt_days": 3},
}
dicts = []
for issue_time, issue_group in df_existing.groupby("issuance"):
    if False:
        continue
    for atcf_id, group in issue_group.groupby("id"):
        cols = ["latitude", "longitude", "maxwind"]
        df_interp = (
            group.set_index("validTime")[cols]
            .resample("30min")
            .interpolate()
            .reset_index()
        )
        gdf = gpd.GeoDataFrame(
            df_interp,
            geometry=gpd.points_from_xy(
                df_interp.longitude, df_interp.latitude
            ),
            crs="EPSG:4326",
        ).to_crs(3857)
        gdf["distance"] = gdf.geometry.distance(adm0.iloc[0].geometry) / 1000
        gdf["leadtime"] = gdf["validTime"] - issue_time
        closest_date = None
        gdf_dist = gdf[gdf["distance"] < D_THRESH]
        for thresh in threshs:
            pass
        dicts.append(
            {
                "monitor_id": f"{atcf_id}_{issue_time.isoformat()}",
                "atcf_id": atcf_id,
                "name": group["name"].iloc[0],
                "issue_time": issue_time,
                "min_dist": gdf["distance"].min(),
            }
        )
    if gdf["distance"].min() == 0:
        break

df_new_monitoring = pd.DataFrame(dicts)
df_new_monitoring
```

```python
dicts
```

```python
group["name"].iloc[0]
```

```python
gdf.plot()
```

```python
"noaa/nhc/forecasted_tracks.csv"
```
