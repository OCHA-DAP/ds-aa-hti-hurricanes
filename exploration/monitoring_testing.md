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

from src.datasources import nhc, codab, chirps_gefs, imerg
from src.utils import blob
from src.constants import *
```

```python
manual_forecasts = pd.read_csv(
    "../temp/manual_forecasts.csv", parse_dates=["issuance", "validTime"]
)
manual_forecasts
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
df_gefs = df_gefs_all[
    df_gefs_all["issue_date"] == gefs_most_recent_date
].copy()
```

```python
# df_gefs_hist = chirps_gefs.load_chirps_gefs_mean_daily()
```

```python
df_gefs["roll2_sum"] = (
    df_gefs["mean"].rolling(window=2, center=True, min_periods=1).sum()
)
df_gefs
```

```python
df_existing = nhc.load_recent_glb_forecasts()
df_existing = df_existing[df_existing["basin"] == "al"]
df_existing = df_existing[df_existing["name"] == "Beryl"]
df_existing["maxwind"] = df_existing["maxwind"].astype(int)
df_existing["validTime"] = pd.to_datetime(df_existing["validTime"])
```

```python
df_existing = df_existing.drop_duplicates()
```

```python
# df_gefs = df_gefs_hist
gefs_most_recent_date = df_gefs_all["issue_date"].max()

threshs = {
    "readiness": {"p": 35, "s": 34, "lt_days": 5},
    "action": {"p": 42, "s": 64, "lt_days": 3},
}
dicts = []
for issue_time, issue_group in df_existing.groupby("issuance"):
    if False:
        continue
    for atcf_id, group in issue_group.groupby("id"):
        gefs_issuetime = df_gefs_all[
            df_gefs_all["issue_date"] == gefs_most_recent_date
        ].copy()
        # gefs_issuetime = df_gefs[
        #     df_gefs["issue_date"].dt.date == issue_time.date()
        # ].copy()
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
        landfall_row = gdf.loc[gdf["distance"].idxmin()]
        time_to_landfall = landfall_row["leadtime"]

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
                "monitor_id": f"{atcf_id}_{issue_time.isoformat().split('+')[0]}",
                "atcf_id": atcf_id,
                "name": group["name"].iloc[0],
                "issue_time": issue_time,
                "time_to_closest": time_to_landfall,
                "closest_s": landfall_row["maxwind"],
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
df_new_monitoring.iloc[-5:]
```

```python
df_new_monitoring
```

```python
most_recent_point = df_new_monitoring.loc[
    df_new_monitoring["issue_time"].idxmax()
]
print(most_recent_point)
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/stats_{D_THRESH}km.csv"
stats = blob.load_csv_from_blob(blob_name)
```

```python
def sid_color(sid):
    color = "blue"
    if sid in CERF_SIDS:
        color = "red"
    return color


stats["marker_size"] = stats["affected_population"] / 6e2
stats["marker_size"] = stats["marker_size"].fillna(1)
stats["color"] = stats["sid"].apply(sid_color)
```

```python
rain_col = "max_roll2_sum_rain"
current_p = most_recent_point["action_p"]
current_s = most_recent_point["action_s"]
current_name = most_recent_point["name"]

date_str = (
    f'Prévision {most_recent_point["issue_time"].strftime("%Hh%M %d %b UTC")}'
)

rain_source_str = "IMERG" if "imerg" in rain_col else "CHIRPS"
rain_ymax = 170 if "imerg" in rain_col else 100

for en_mo, fr_mo in FRENCH_MONTHS.items():
    date_str = date_str.replace(en_mo, fr_mo)

fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

ax.scatter(
    stats["max_wind"],
    stats[rain_col],
    s=stats["marker_size"],
    c=stats["color"],
    alpha=0.5,
    edgecolors="none",
)

for j, txt in enumerate(
    stats["name"].str.capitalize() + "\n" + stats["year"].astype(str)
):
    ax.annotate(
        txt.capitalize(),
        (stats["max_wind"][j] + 0.5, stats[rain_col][j]),
        ha="left",
        va="center",
        fontsize=7,
    )

ax.scatter(
    [current_s],
    [current_p],
    marker="x",
    color=CHD_GREEN,
    linewidths=3,
    s=100,
)
ax.annotate(
    f"   {current_name}\n",
    (current_s, current_p),
    va="center",
    ha="left",
    color=CHD_GREEN,
    fontweight="bold",
)
ax.annotate(
    f"\n   {date_str}",
    (current_s, current_p),
    va="center",
    ha="left",
    color=CHD_GREEN,
    fontstyle="italic",
)

for rain_thresh, s_thresh in zip([42], [64]):
    ax.axvline(x=s_thresh, color="lightgray", linewidth=0.5)
    ax.axhline(y=rain_thresh, color="lightgray", linewidth=0.5)
    ax.fill_between(
        np.arange(s_thresh, 200, 1),
        rain_thresh,
        200,
        color="gold",
        alpha=0.2,
        zorder=-1,
    )

ax.annotate(
    "\nZone de déclenchement   ",
    (155, rain_ymax),
    ha="right",
    va="top",
    color="orange",
    fontweight="bold",
)
ax.annotate(
    "\n\nAllocations CERF en rouge   ",
    (155, rain_ymax),
    ha="right",
    va="top",
    color="crimson",
    fontstyle="italic",
)

ax.set_xlim(right=155, left=0)
ax.set_ylim(top=rain_ymax, bottom=0)

ax.set_xlabel("Vitesse de vent maximum (noeuds)")
ax.set_ylabel(
    "Précipitations pendant deux jours consécutifs maximum,\n"
    f"moyenne sur toute la superficie (mm, {rain_source_str})"
)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title(
    f"Comparaison de précipitations, vent, et impact\n"
    f"Seuil de distance = {D_THRESH} km"
)
```

```python
# observational

obsv_tracks = nhc.load_recent_glb_obsv()
obsv_tracks = obsv_tracks[obsv_tracks["name"] == "Beryl"]
```

```python
obsv_rain = imerg.load_imerg_mean(version=7, recent=True)
obsv_rain["roll2_sum"] = (
    obsv_rain["mean"].rolling(window=2, center=True, min_periods=1).sum()
)
```

```python
obsv_rain
```

```python
cols = ["latitude", "longitude", "intensity", "pressure"]
df_interp = (
    obsv_tracks.set_index("lastUpdate")[cols]
    .resample("30min")
    .interpolate()
    .reset_index()
)
gdf = gpd.GeoDataFrame(
    data=df_interp,
    geometry=gpd.points_from_xy(df_interp["longitude"], df_interp["latitude"]),
    crs=4326,
)
```

```python
obsv_tracks
```

```python
gdf["hti_distance"] = (
    gdf.to_crs(3857).geometry.distance(adm0.iloc[0].geometry) / 1000
)
```

```python
gdf_dist = gdf[gdf["hti_distance"] < D_THRESH]
max_s = gdf_dist["intensity"].max()
```

```python
gdf_dist
```

```python
start_day = pd.Timestamp(gdf_dist["lastUpdate"].min().date())
end_day_late = pd.Timestamp(
    gdf_dist["lastUpdate"].max().date() + pd.Timedelta(days=1)
)
obsv_rain_f = obsv_rain[
    (obsv_rain["date"] >= start_day) & (obsv_rain["date"] <= end_day_late)
]
max_p = obsv_rain_f["roll2_sum"].max()
```

```python
print(max_s, max_p)
```

```python
rain_col = "max_roll2_sum_rain_imerg"
current_p = max_p
current_s = max_s
current_name = obsv_tracks.iloc[-1]["name"]

date_str = f'Obsv. à partir de {obsv_rain["date"].max().strftime("%d %b")}'
rain_source_str = "IMERG" if "imerg" in rain_col else "CHIRPS"
rain_ymax = 170 if "imerg" in rain_col else 100

for en_mo, fr_mo in FRENCH_MONTHS.items():
    date_str = date_str.replace(en_mo, fr_mo)

fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

ax.scatter(
    stats["max_wind"],
    stats[rain_col],
    s=stats["marker_size"],
    c=stats["color"],
    alpha=0.5,
    edgecolors="none",
)

for j, txt in enumerate(
    stats["name"].str.capitalize() + "\n" + stats["year"].astype(str)
):
    ax.annotate(
        txt.capitalize(),
        (stats["max_wind"][j] + 0.5, stats[rain_col][j]),
        ha="left",
        va="center",
        fontsize=7,
    )

ax.scatter(
    [current_s],
    [current_p],
    marker="x",
    color=CHD_GREEN,
    linewidths=3,
    s=100,
)
ax.annotate(
    f"   {current_name}\n",
    (current_s, current_p),
    va="center",
    ha="left",
    color=CHD_GREEN,
    fontweight="bold",
)
ax.annotate(
    f"\n   {date_str}",
    (current_s, current_p),
    va="center",
    ha="left",
    color=CHD_GREEN,
    fontstyle="italic",
)

for rain_thresh, s_thresh in zip([70], [50]):
    ax.axvline(x=s_thresh, color="lightgray", linewidth=0.5)
    ax.axhline(y=rain_thresh, color="lightgray", linewidth=0.5)
    ax.fill_between(
        np.arange(s_thresh, 200, 1),
        rain_thresh,
        200,
        color="gold",
        alpha=0.2,
        zorder=-1,
    )

ax.annotate(
    "\nZone de déclenchement   ",
    (155, rain_ymax),
    ha="right",
    va="top",
    color="orange",
    fontweight="bold",
)
ax.annotate(
    "\n\nAllocations CERF en rouge   ",
    (155, rain_ymax),
    ha="right",
    va="top",
    color="crimson",
    fontstyle="italic",
)

ax.set_xlim(right=155, left=0)
ax.set_ylim(top=rain_ymax, bottom=0)

ax.set_xlabel("Vitesse de vent maximum (noeuds)")
ax.set_ylabel(
    "Précipitations pendant deux jours consécutifs maximum,\n"
    f"moyenne sur toute la superficie (mm, {rain_source_str})"
)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title(
    f"Comparaison de précipitations, vent, et impact\n"
    f"Seuil de distance = {D_THRESH} km"
)
```

```python

```
