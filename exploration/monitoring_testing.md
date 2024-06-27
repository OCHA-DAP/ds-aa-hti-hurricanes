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
df_gefs_hist = chirps_gefs.load_chirps_gefs_mean_daily()
```

```python
df_gefs_hist
```

```python
df_existing = nhc.load_recent_glb_forecasts()
df_existing = df_existing[df_existing["basin"] == "al"]
```

```python
gefs_issuetime["roll2_sum"] = (
    gefs_issuetime["mean"].rolling(window=2, center=True, min_periods=1).sum()
)
gefs_issuetime
```

```python
df_gefs = df_gefs_hist

threshs = {
    "readiness": {"p": 35, "s": 34, "lt_days": 5},
    "action": {"p": 42, "s": 64, "lt_days": 3},
}
dicts = []
for issue_time, issue_group in df_existing.groupby("issuance"):
    if False:
        continue
    for atcf_id, group in issue_group.groupby("id"):
        gefs_issuetime = df_gefs[
            df_gefs["issue_date"].dt.date == issue_time.date()
        ].copy()
        gefs_issuetime["roll2_sum"] = (
            gefs_issuetime["mean"]
            .rolling(window=2, center=True, min_periods=1)
            .sum()
        )
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
        time_to_landfall = gdf.loc[gdf["distance"].idxmin()]["leadtime"]
        gdf_dist = gdf[gdf["distance"] < D_THRESH]

        # action
        gdf_action = gdf_dist[
            gdf_dist["leadtime"]
            <= pd.Timedelta(days=threshs["action"]["lt_days"])
        ]
        action_start_date = gdf_action["validTime"].min().date()
        action_end_date = gdf_action["validTime"].max().date() + pd.Timedelta(
            days=1
        )
        gefs_action = gefs_issuetime[
            (gefs_issuetime["valid_date"].dt.date >= action_start_date)
            & (gefs_issuetime["valid_date"].dt.date <= action_end_date)
        ]
        action_s = gdf_action["maxwind"].max()
        action_p = gefs_action["roll2_sum"].max()
        action_trigger = (action_s >= threshs["action"]["s"]) & (
            action_p >= threshs["action"]["p"]
        )

        # readiness
        gdf_readiness = gdf_dist[
            gdf_dist["leadtime"]
            <= pd.Timedelta(days=threshs["readiness"]["lt_days"])
        ]
        readiness_start_date = gdf_readiness["validTime"].min().date()
        readiness_end_date = gdf_readiness[
            "validTime"
        ].max().date() + pd.Timedelta(days=1)
        gefs_readiness = gefs_issuetime[
            (gefs_issuetime["valid_date"].dt.date >= readiness_start_date)
            & (gefs_issuetime["valid_date"].dt.date <= readiness_end_date)
        ]
        readiness_s = gdf_readiness["maxwind"].max()
        readiness_p = gefs_readiness["roll2_sum"].max()
        readiness_trigger = (readiness_s >= threshs["readiness"]["s"]) & (
            readiness_p >= threshs["readiness"]["p"]
        )

        dicts.append(
            {
                "monitor_id": f"{atcf_id}_{issue_time.isoformat()}",
                "atcf_id": atcf_id,
                "name": group["name"].iloc[0],
                "issue_time": issue_time,
                "time_to_closest": time_to_landfall,
                "past_cutoff": time_to_landfall
                < pd.Timedelta(hours=LT_CUTOFF_HRS),
                "min_dist": gdf["distance"].min(),
                "action_s": action_s,
                "action_p": action_p,
                "action_trigger": action_trigger,
                "readiness_s": readiness_s,
                "readiness_p": readiness_p,
                "readiness_trigger": readiness_trigger,
            }
        )
    if gdf["distance"].min() == 0:
        pass
        # break

df_new_monitoring = pd.DataFrame(dicts)
df_new_monitoring
```

```python
gefs_action
```

```python
gdf_action
```

```python
time_to_landfall
```

```python
"noaa/nhc/forecasted_tracks.csv"
```
