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
df_monitoring = monitoring_utils.load_existing_monitoring_points(
    fcast_obsv="fcast"
)
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
