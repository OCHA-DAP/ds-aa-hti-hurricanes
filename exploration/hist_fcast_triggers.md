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

# Historical forecast triggers

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import pandas as pd
from tqdm.notebook import tqdm

from src.datasources import nhc, codab, chirps_gefs, chirps, ibtracs
from src.utils import blob
```

```python
sid_atcf = ibtracs.load_ibtracs_sid_atcf_names()
sid_atcf.loc[:, "name"] = sid_atcf["name"].str.capitalize()
sid_atcf.loc[:, "usa_atcf_id"] = sid_atcf["usa_atcf_id"].str.lower()
sid_atcf = sid_atcf.rename(columns={"usa_atcf_id": "atcf_id"})
```

```python
obsv_tracks = ibtracs.load_hti_distances()
obsv_tracks = obsv_tracks.merge(sid_atcf[["sid", "atcf_id"]], on="sid")
```

```python
obsv_tracks
```

```python
trigger_str = "d230_s50_AND_max_roll2_sum_rain40"
filename = f"{trigger_str}_triggers.csv"
obsv_triggers = pd.read_csv(ibtracs.IBTRACS_HTI_PROC_DIR / filename)
obsv_triggers = obsv_triggers.merge(sid_atcf[["sid", "atcf_id"]])
```

```python
def closest_date(sid):
    dff = obsv_tracks[obsv_tracks["sid"] == sid]
    dff = dff[dff["distance (m)"] == dff["distance (m)"].min()]
    return dff["time"].min()


obsv_triggers["closest_time"] = obsv_triggers["sid"].apply(closest_date)
```

```python
obsv_triggers
```

```python
D_THRESH = 230
P_THRESH = 40
S_THRESH = 50
```

```python
rain = chirps_gefs.load_chirps_gefs_mean_daily()
```

```python
rain["roll2_sum_bw"] = (
    rain.groupby("issue_date")["mean"]
    .rolling(window=2, center=True, min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)
```

```python
rain
```

```python
rain["lt"] = rain["valid_date"] - rain["issue_date"]
```

```python
rain.groupby("lt")["mean"].mean().plot()
```

```python
obsv_rain = chirps.load_raster_stats()
obsv_rain["roll2_sum_bw"] = (
    obsv_rain["mean"]
    .rolling(window=2, center=True, min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)
```

```python
all_tracks = nhc.load_hti_distances()
```

```python
close_tracks = all_tracks[all_tracks["hti_distance_km"] < D_THRESH]
```

```python
close_atcf_ids = close_tracks["atcf_id"].unique()
```

```python
tracks = all_tracks[all_tracks["atcf_id"].isin(close_atcf_ids)]
```

```python
lts = {"readiness": pd.Timedelta(days=5), "action": pd.Timedelta(days=3)}

dicts = []
for atcf_id, storm_group in tqdm(tracks.groupby("atcf_id")):
    obsv_tracks_f = obsv_tracks[obsv_tracks["atcf_id"] == atcf_id]
    for issue_time, issue_group in storm_group.groupby("issue_time"):
        rain_i = rain[
            rain["issue_date"].astype(str) == issue_time.strftime("%Y-%m-%d")
        ]
        dff = issue_group[issue_group["hti_distance_km"] < D_THRESH]
        for lt_name, lt in lts.items():
            dff_lt = dff[dff["valid_time"] <= issue_time + lt]
            start_date = pd.Timestamp(dff_lt["valid_time"].min().date())
            end_date = pd.Timestamp(
                dff_lt["valid_time"].max().date() + pd.Timedelta(days=1)
            )
            lt_rain = rain_i[
                (rain_i["valid_date"] >= start_date)
                & (rain_i["valid_date"] <= end_date)
            ]["roll2_sum_bw"].max()
            lt_wind = dff_lt["windspeed"].max()
            dicts.append(
                {
                    "atcf_id": atcf_id,
                    "issue_time": issue_time,
                    "lt_name": lt_name,
                    "roll2_rain": lt_rain,
                    "wind": lt_wind,
                }
            )

monitors = pd.DataFrame(dicts)
print(len(monitors))
monitors = monitors.merge(sid_atcf)
monitors
```

```python
lt_threshs = {
    "readiness": {"p": 1 * P_THRESH, "s": S_THRESH},
    "action": {"p": 1 * P_THRESH, "s": S_THRESH},
}

dicts = []
for atcf_id, group in monitors.groupby("atcf_id"):
    for lt_name, threshs in lt_threshs.items():
        dff = group[group["lt_name"] == lt_name]
        dff_t = dff[
            (dff["roll2_rain"] >= threshs.get("p"))
            & (dff["wind"] >= threshs.get("s"))
        ]
        if not dff_t.empty:
            trig_date = dff_t["issue_time"].min()
            dicts.append(
                {
                    "atcf_id": atcf_id,
                    "trig_date": trig_date,
                    "lt_name": lt_name,
                }
            )

triggers = pd.DataFrame(dicts)
triggers = (
    triggers.pivot(index="atcf_id", columns="lt_name", values="trig_date")
    .reset_index()
    .rename_axis(None, axis=1)
)
triggers = triggers.merge(
    obsv_triggers[["atcf_id", "closest_time", "name"]], how="left"
)
for lt_name in lt_threshs:
    triggers[f"{lt_name}_lt"] = triggers["closest_time"] - triggers[lt_name]
```

```python
triggers["readiness_lt"].mean()
```

```python
triggers["action_lt"].mean()
```

```python
# Ike
monitors.set_index("atcf_id").loc["al092008"].dropna()
```

```python
# Irma
monitors.set_index("atcf_id").loc["al112017"].dropna()
```

```python
# Sandy
monitors.set_index("atcf_id").loc["al182012"].dropna()
```
