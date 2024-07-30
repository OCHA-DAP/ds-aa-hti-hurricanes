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
from src.monitoring import monitoring_utils
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
monitoring_utils.update_fcast_monitoring(clobber=False)
```

```python
df_tracks = nhc.load_recent_glb_forecasts()
df_tracks = df_tracks[df_tracks["basin"] == "al"]
```

```python
df_tracks["leadtime"] = df_tracks["validTime"] - df_tracks["issuance"]
```

```python
df_monitoring = monitoring_utils.load_existing_monitoring_points(
    fcast_obsv="fcast"
)
```

```python
df_monitoring
```

```python
most_recent_point = df_monitoring.loc[df_monitoring["issue_time"].idxmax()]
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
most_recent_point["min_dist"] >= D_THRESH
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

if most_recent_point["min_dist"] >= D_THRESH:
    rect = plt.Rectangle(
        (0, 0),
        1,
        1,
        transform=ax.transAxes,
        color="white",
        alpha=0.7,
        zorder=3,
    )
    ax.add_patch(rect)

    # Add white text in the middle
    ax.text(
        0.5,
        0.5,
        f"{current_name} pas prévu d'être\nà moins de {D_THRESH} de Haïti",
        fontsize=30,
        color="grey",
        ha="center",
        va="center",
        transform=ax.transAxes,
    )
```

```python
clobber = False
```

```python
# observational

obsv_tracks = nhc.load_recent_glb_obsv()
obsv_tracks = obsv_tracks[obsv_tracks["basin"] == "al"]
obsv_tracks = obsv_tracks.rename(columns={"id": "atcf_id"})
obsv_tracks = obsv_tracks.sort_values("lastUpdate")
# obsv_tracks = obsv_tracks[obsv_tracks["name"] == "Beryl"]
```

```python
obsv_tracks[obsv_tracks["name"] == "Chris"]
```

```python
obsv_rain = imerg.load_imerg_mean(version=7, recent=True)
obsv_rain["roll2_sum"] = (
    obsv_rain["mean"].rolling(window=2, center=True, min_periods=1).sum()
)
obsv_rain["issue_time"] = obsv_rain["date"].apply(
    lambda x: x.tz_localize("UTC")
) + pd.Timedelta(hours=15, days=1)
```

```python
obsv_rain
```

```python
df_existing_monitoring = monitoring_utils.load_existing_monitoring_points(
    "obsv"
)
```

```python
df_existing_monitoring
```

```python
cols = ["latitude", "longitude", "intensity", "pressure"]

dicts = []
for atcf_id, group in obsv_tracks.groupby("atcf_id"):
    df_interp = (
        group.set_index("lastUpdate")[cols]
        .resample("30min")
        .interpolate()
        .reset_index()
    )
    gdf = gpd.GeoDataFrame(
        data=df_interp,
        geometry=gpd.points_from_xy(
            df_interp["longitude"], df_interp["latitude"]
        ),
        crs=4326,
    )
    gdf["hti_distance"] = (
        gdf.to_crs(3857).geometry.distance(adm0.iloc[0].geometry) / 1000
    )
    for issue_time in obsv_rain["issue_time"]:
        monitor_id = f"{atcf_id}_obsv_{issue_time.isoformat().split('+')[0]}"
        if (
            monitor_id in df_existing_monitoring["monitor_id"].unique()
            and not clobber
        ):
            print(f"already monitored for {monitor_id}")
            continue
        rain_recent = obsv_rain[obsv_rain["issue_time"] <= issue_time]
        gdf_recent = gdf[gdf["lastUpdate"] <= issue_time]
        if gdf_recent.empty:
            # skip as storm is not active yet
            continue
        if rain_recent["date"].max().date() - gdf_recent[
            "lastUpdate"
        ].max().date() > pd.Timedelta(days=1):
            # skip as storm is not longer active
            continue

        name = group[group["lastUpdate"] <= issue_time].iloc[-1]["name"]

        # closest pass
        landfall_row = gdf_recent.loc[gdf_recent["hti_distance"].idxmin()]
        closest_s = landfall_row["intensity"]
        landfall_start_day = landfall_row["lastUpdate"].date()
        landfall_end_day_late = landfall_start_day + pd.Timedelta(days=1)
        obsv_rain_landfall = rain_recent[
            (rain_recent["date"].dt.date >= landfall_start_day)
            & (rain_recent["date"].dt.date <= landfall_end_day_late)
        ]
        closest_p = obsv_rain_landfall["roll2_sum"].max()

        # obsv trigger
        gdf_dist = gdf_recent[gdf_recent["hti_distance"] < D_THRESH]
        max_s = gdf_dist["intensity"].max()
        start_day = pd.Timestamp(gdf_dist["lastUpdate"].min().date())
        end_day_late = pd.Timestamp(
            gdf_dist["lastUpdate"].max().date() + pd.Timedelta(days=1)
        )
        # rainfall is no longer relevant if past the date the storm left
        # the trigger zone
        rainfall_relevant = rain_recent["date"].max() <= end_day_late
        obsv_rain_f = rain_recent[
            (rain_recent["date"] >= start_day)
            & (rain_recent["date"] <= end_day_late)
        ]
        max_p = obsv_rain_f["roll2_sum"].max()
        obsv_trigger = (max_p > monitoring_utils.THRESHS["obsv"]["p"]) & (
            max_s > monitoring_utils.THRESHS["obsv"]["s"]
        )
        dicts.append(
            {
                "monitor_id": monitor_id,
                "atcf_id": atcf_id,
                "name": name,
                "issue_time": issue_time,
                "min_dist": gdf_recent["hti_distance"].min(),
                "closest_s": closest_s,
                "closest_p": closest_p,
                "obsv_s": max_s,
                "obsv_p": max_p,
                "rainfall_relevant": rainfall_relevant,
                "obsv_trigger": obsv_trigger,
            }
        )

df_new_monitoring = pd.DataFrame(dicts)
```

```python
df_new_monitoring
```

```python
if clobber:
    df_monitoring_combined = df_new_monitoring
else:
    df_monitoring_combined = pd.concat(
        [df_existing_monitoring, df_new_monitoring]
    )
```

```python
df_monitoring_combined
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/monitoring/hti_obsv_monitoring.parquet"
blob.upload_parquet_to_blob(blob_name, df_monitoring_combined, index=False)
```

```python
dummy_df_monitoring
```

```python
gdf
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
