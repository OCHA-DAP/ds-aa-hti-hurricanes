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

# Exposure calculation

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import ocha_stratus as stratus
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import xarray as xr
from matplotlib.patches import Patch
from rioxarray.exceptions import NoDataInBounds
from tqdm.auto import tqdm
from matplotlib.ticker import EngFormatter
from dask.diagnostics import ProgressBar

from src.datasources import codab, chirps_gefs
from src.constants import *
from src.utils.blob import PROJECT_PREFIX
from src.utils.raster import upsample_dataarray
from src.utils import plotting
```

```python
adm0 = codab.load_codab_from_blob(admin_level=0)
```

```python
adm1 = codab.load_codab_from_blob(admin_level=1)
```

```python
adm2 = codab.load_codab_from_blob(admin_level=2)
```

```python
blob_name = "ghsl/pop/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0.tif"
da_global = stratus.open_blob_cog(blob_name, container_name="raster").squeeze(
    drop=True
)
```

```python
# clip to box (need to do this first, otherwise Python crashes on normal .rio.clip)
minx, miny, maxx, maxy = adm0.total_bounds
da_clip_box = da_global.rio.clip_box(
    minx=minx, miny=miny, maxx=maxx, maxy=maxy
)
```

```python
da_ghsl = da_clip_box.rio.clip(adm0.geometry)
```

```python
da_ghsl.attrs["_FillValue"] = np.nan
```

```python
da_ghsl = da_ghsl.where(da_ghsl >= 0)
```

```python
da_ghsl = da_ghsl.compute()
```

```python
blob_name = (
    "ds-aa-cub-hurricanes/raw/noaa/nhc/wind_history/al132025_best_track.zip"
)
gdf_wind = stratus.load_shp_from_blob(
    blob_name, shapefile="AL132025_windswath.shp"
)
```

```python
gdf_wind["Wind Speed (knots)"] = (
    gdf_wind["RADII"].astype(int).astype("category")
)
```

```python
# get IMERG
query = """
SELECT *
FROM public.imerg
WHERE pcode = 'HT'
"""
with stratus.get_engine(stage="prod").connect() as con:
    df_imerg = pd.read_sql(query, con)
```

```python
df_imerg["valid_date"] = pd.to_datetime(df_imerg["valid_date"])
df_imerg = df_imerg.sort_values("valid_date")
```

```python
df_imerg.iloc[-20:].plot(x="valid_date", y="mean")
```

```python
imerg_dates = pd.date_range("2025-10-21", "2025-10-29")
```

```python
imerg_dates
```

```python
IMERG_BLOB_NAME = (
    "imerg/daily/late/v7/processed/imerg-daily-late-{date_str}.tif"
)
```

```python
das = []
for d in imerg_dates:
    blob_name = IMERG_BLOB_NAME.format(date_str=d.date())
    da_in = stratus.open_blob_cog(
        blob_name, stage="prod", container_name="raster"
    )
    da_in["date"] = d
    das.append(da_in)
```

```python
da_imerg = xr.concat(das, dim="date").squeeze(drop=True)
```

```python
total_bounds = adm0.total_bounds
```

```python
da_imerg_clip_box = da_imerg.rio.clip_box(*total_bounds)
```

```python
with ProgressBar():
    da_imerg_clip_box_computed = da_imerg_clip_box.compute()
```

```python
da_imerg_clip = da_imerg_clip_box_computed.rio.clip(
    adm0.geometry, all_touched=True
)
```

```python
da_imerg_clip_box_computed.isel(date=0).plot()
```

```python
da_rainfall = da_imerg_clip.where(da_imerg_clip > 0).sum(dim="date")
```

```python
da_rainfall_up = upsample_dataarray(
    da_rainfall, lat_dim="y", lon_dim="x", resolution=0.005
).rio.clip(adm2.geometry)
```

```python
adm2["adm_label_break"] = adm2["ADM2_FR"].apply(plotting.wrap_text)
```

```python
fig, ax = plt.subplots(dpi=200, figsize=(12, 6))

minx, miny, maxx, maxy = adm2.total_bounds

adm1.boundary.plot(ax=ax, linewidth=0.3, color="k")
adm2.boundary.plot(ax=ax, linewidth=0.1, color="k")

for _, row in adm2.iterrows():
    c = row.geometry.centroid
    ax.annotate(
        row["adm_label_break"],
        (c.x, c.y),
        ha="center",
        va="center",
        fontsize=4,
    )

da_rainfall_up.where(da_rainfall_up >= 0).plot(
    ax=ax,
    cmap="Blues",
    cbar_kwargs={"label": "Précipitations (mm)"},
    vmin=0,
)

alpha = 0.3
color_map = {34: "gold", 50: "crimson", 64: "indigo"}
for value, color in color_map.items():
    gdf_wind[gdf_wind["Wind Speed (knots)"] == value].plot(
        ax=ax, color=color, alpha=alpha, label=f"{value} kt"
    )

handles = [
    Patch(facecolor=color, label=f"{value} kt", alpha=alpha)
    for value, color in color_map.items()
]

ax.legend(
    handles=handles, title="Vent (nœuds)", loc="upper left", frameon=True
)

ax.set_title(
    "Ouragan Melissa historique des vents\n"
    f"et précipitations totales sur {imerg_dates.min().date()} à {imerg_dates.max().date()}"
)

# da_ghsl_aoi.plot(ax=ax, vmax=5, cmap="Greys")
margin = 0.1
ax.set_xlim(minx - margin, maxx + margin)
ax.set_ylim(miny - margin, maxy + margin)
ax.axis("off")
```

```python
fig, ax = plt.subplots(dpi=200, figsize=(12, 6))

minx, miny, maxx, maxy = adm2.total_bounds

adm1.boundary.plot(ax=ax, linewidth=0.3, color="k")
# adm2.boundary.plot(ax=ax, linewidth=0.1, color="k")

for _, row in adm1.iterrows():
    c = row.geometry.centroid
    ax.annotate(
        row["ADM1_FR"],
        (c.x, c.y),
        ha="center",
        va="center",
        fontsize=12,
    )

da_rainfall_up.where(da_rainfall_up >= 0).plot(
    ax=ax,
    cmap="Blues",
    cbar_kwargs={"label": "Précipitations (mm)"},
    vmin=0,
)

alpha = 0.3
color_map = {34: "gold", 50: "crimson", 64: "indigo"}
for value, color in color_map.items():
    gdf_wind[gdf_wind["Wind Speed (knots)"] == value].plot(
        ax=ax, color=color, alpha=alpha, label=f"{value} kt"
    )

handles = [
    Patch(facecolor=color, label=f"{value} kt", alpha=alpha)
    for value, color in color_map.items()
]

ax.legend(
    handles=handles, title="Vent (nœuds)", loc="upper left", frameon=True
)

ax.set_title(
    "Ouragan Melissa historique des vents\n"
    f"et précipitations totales sur {imerg_dates.min().date()} à {imerg_dates.max().date()}"
)

# da_ghsl_aoi.plot(ax=ax, vmax=5, cmap="Greys")
margin = 0.1
ax.set_xlim(minx - margin, maxx + margin)
ax.set_ylim(miny - margin, maxy + margin)
ax.axis("off")
```

```python
levels = [25, 50, 100, 150, 200, 300, 400, 500, 750]
colors = [
    "lawngreen",
    "limegreen",
    "yellow",
    "gold",
    "darkorange",
    "red",
    "firebrick",
    "magenta",
    "darkmagenta",
]
cbar_kwargs = {
    "label": "Précipitations (mm)",  # Set label for the colorbar
    "shrink": 0.8,  # Shrink the colorbar to 80% of its default size
}
```

```python
fig, ax = plt.subplots(dpi=200, figsize=(12, 6))

minx, miny, maxx, maxy = adm2.total_bounds

adm1.boundary.plot(ax=ax, linewidth=0.4, color="k")
adm2.boundary.plot(ax=ax, linewidth=0.1, color="k")

for _, row in adm2.iterrows():
    c = row.geometry.centroid
    ax.annotate(
        row["adm_label_break"],
        (c.x, c.y),
        ha="center",
        va="center",
        fontsize=4,
    )

da_rainfall_up.where(da_rainfall_up > 0).plot(
    ax=ax,
    levels=levels,
    colors=colors,
    extend="max",
    cbar_kwargs=cbar_kwargs,
    alpha=0.7,
)

alpha = 0.4
for wind, color in color_map.items():
    gdf_wind[gdf_wind["RADII"] == wind].boundary.plot(
        ax=ax, color=color, alpha=alpha
    )

# Create legend using the same color map
legend_elements = [
    Patch(facecolor="white", edgecolor=color, label=f"{wind}", alpha=alpha)
    for wind, color in color_map.items()
]

ax.legend(handles=legend_elements, title="Vent (nœuds)", loc="upper left")

ax.set_title(
    "Ouragan Melissa historique des vents\n"
    f"et précipitations totales sur {imerg_dates.min().date()} à {imerg_dates.max().date()}"
)

# da_ghsl_aoi.plot(ax=ax, vmax=5, cmap="Greys")
margin = 0.1
ax.set_xlim(minx - margin, maxx + margin)
ax.set_ylim(miny - margin, maxy + margin)
ax.axis("off")
```

```python
fig, ax = plt.subplots(dpi=200, figsize=(12, 6))

minx, miny, maxx, maxy = adm2.total_bounds

adm1.boundary.plot(ax=ax, linewidth=0.4, color="k")
# adm2.boundary.plot(ax=ax, linewidth=0.1, color="k")

for _, row in adm1.iterrows():
    c = row.geometry.centroid
    ax.annotate(
        row["ADM1_FR"],
        (c.x, c.y),
        ha="center",
        va="center",
        fontsize=12,
    )

da_rainfall_up.where(da_rainfall_up > 0).plot(
    ax=ax,
    levels=levels,
    colors=colors,
    extend="max",
    cbar_kwargs=cbar_kwargs,
    alpha=0.7,
)

alpha = 0.4
for wind, color in color_map.items():
    gdf_wind[gdf_wind["RADII"] == wind].boundary.plot(
        ax=ax, color=color, alpha=alpha
    )

# Create legend using the same color map
legend_elements = [
    Patch(facecolor="white", edgecolor=color, label=f"{wind}", alpha=alpha)
    for wind, color in color_map.items()
]

ax.legend(handles=legend_elements, title="Vent (nœuds)", loc="upper left")

ax.set_title(
    "Ouragan Melissa historique des vents\n"
    f"et précipitations totales sur {imerg_dates.min().date()} à {imerg_dates.max().date()}"
)

# da_ghsl_aoi.plot(ax=ax, vmax=5, cmap="Greys")
margin = 0.1
ax.set_xlim(minx - margin, maxx + margin)
ax.set_ylim(miny - margin, maxy + margin)
ax.axis("off")
```

```python
dicts = []
for pcode, group in tqdm(adm2.groupby("ADM2_PCODE")):
    ghsl_adm2 = da_ghsl.rio.clip(group.geometry)
    dicts.append({"ADM2_PCODE": pcode, "total_pop": int(ghsl_adm2.sum())})
```

```python
df_adm2_pop = pd.DataFrame(dicts)
```

```python
dicts = []
for pcode, group in tqdm(adm2.groupby("ADM2_PCODE")):
    ghsl_adm2 = da_ghsl.rio.clip(group.geometry)
    for speed, row in gdf_wind.set_index("RADII").iterrows():
        try:
            ghsl_adm2_speed = ghsl_adm2.rio.clip([row.geometry])
            pop_exp = int(ghsl_adm2_speed.sum())
        except NoDataInBounds:
            pop_exp = 0
        dicts.append(
            {"ADM2_PCODE": pcode, "speed": int(speed), "pop_exp": pop_exp}
        )
```

```python
df_adm2_exp_raw = pd.DataFrame(dicts)
```

```python
df_adm2_exp = df_adm2_exp_raw.pivot(
    index="ADM2_PCODE", columns="speed", values="pop_exp"
)

df_adm2_exp = df_adm2_exp.rename(
    columns={x: f"exp_{x}_knots" for x in df_adm2_exp.columns}
)
```

```python
df_adm2_exp = df_adm2_exp.reset_index().merge(df_adm2_pop, how="right")
```

```python
df_adm2_exp["exp_34_knots"] = df_adm2_exp[
    ["exp_34_knots", "exp_50_knots", "exp_64_knots"]
].sum(axis=1)
df_adm2_exp["exp_50_knots"] = df_adm2_exp[
    ["exp_50_knots", "exp_64_knots"]
].sum(axis=1)
```

```python
df_adm2_exp = df_adm2_exp.fillna(0)
df_adm2_exp = df_adm2_exp.set_index("ADM2_PCODE")
df_adm2_exp = df_adm2_exp.astype(int).reset_index()
```

```python
df_adm2_exp
```

```python
da_rainfall_interp = da_rainfall.interp_like(
    da_ghsl, method="nearest", kwargs={"fill_value": "extrapolate"}
).squeeze(drop=True)
```

```python
rain_threshs = [100, 200, 300, 400, 500]
```

```python
dicts = []
for pcode, group in tqdm(adm2.groupby("ADM2_PCODE")):
    ghsl_adm2 = da_ghsl.rio.clip(group.geometry)
    for rain_thresh in rain_threshs:
        da_ghsl_rain_thresh = ghsl_adm2.where(
            da_rainfall_interp >= rain_thresh
        )
        dicts.append(
            {
                "ADM2_PCODE": pcode,
                "rain_thresh": rain_thresh,
                "pop_exp": int(da_ghsl_rain_thresh.sum()),
            }
        )
```

```python
df_adm2_exp_rain_raw = pd.DataFrame(dicts)
```

```python
df_adm2_exp_rain = df_adm2_exp_rain_raw.pivot(
    index="ADM2_PCODE", columns="rain_thresh", values="pop_exp"
)

df_adm2_exp_rain = df_adm2_exp_rain.rename(
    columns={x: f"exp_{x}_mm" for x in df_adm2_exp_rain.columns}
)

df_adm2_exp_rain = df_adm2_exp_rain.reset_index().merge(
    df_adm2_pop, how="right"
)
```

```python
df_adm2_exp_rain = df_adm2_exp_rain.fillna(0)
df_adm2_exp_rain = df_adm2_exp_rain.set_index("ADM2_PCODE")
df_adm2_exp_rain = df_adm2_exp_rain.astype(int).reset_index()
```

```python
df_exp = df_adm2_exp.merge(df_adm2_exp_rain)
```

```python
df_exp
```

```python
df_out = adm2[["ADM1_PCODE", "ADM1_FR", "ADM2_PCODE", "ADM2_FR"]].merge(df_exp)

df_out = df_out[
    [x for x in df_out.columns if x != "total_pop"] + ["total_pop"]
]

df_out = df_out.sort_values(["ADM1_FR", "ADM2_FR"])

save_path = "temp/hti_melissa_adm2_wind_rain_exposure.csv"
df_out.to_csv(save_path, index=False, encoding="utf-8-sig")

save_path = "temp/hti_melissa_adm2_wind_rain_exposure.xlsx"
df_out.to_excel(save_path, index=False)
```

```python
blob_name = (
    f"{PROJECT_PREFIX}/processed/hti_melissa_adm2_wind_rain_exposure.parquet"
)
stratus.upload_parquet_to_blob(df_out, blob_name)
```

```python
df_exp.mean(numeric_only=True).plot.bar()
```

```python
df_plot = df_exp.merge(adm2)
```

```python
df_plot["adm_label"] = df_plot["ADM2_FR"] + " (" + df_plot["ADM1_FR"] + ")"
```

```python
high_rain = 400

df_plot[f"max_{high_rain}_mm_34_knots"] = df_plot[
    [f"exp_{high_rain}_mm", "exp_34_knots"]
].max(axis=1)
df_plot[f"min_{high_rain}_mm_34_knots"] = df_plot[
    [f"exp_{high_rain}_mm", "exp_34_knots"]
].min(axis=1)

fig, ax = plt.subplots(figsize=(10, 10), dpi=200)

df_plot[df_plot[f"max_{high_rain}_mm_34_knots"] > 0].sort_values(
    f"max_{high_rain}_mm_34_knots"
).plot.barh(
    x="adm_label",
    y=[f"exp_{high_rain}_mm", "exp_34_knots"],
    ax=ax,
    color=["dodgerblue", "darkorange"],
)

ax.legend(
    [f"précip. ≥ {high_rain} mm", "vents ≥ 34 nœuds"],
    title="Population exposée",
)

ax.set_xlabel("Population exposée")
ax.set_ylabel("Arrondissement")
ax.set_title(
    "Haiti : population exposée aux conditions sévères d'ouragan Melissa"
)

# Fine vertical gridlines (since x is now numeric)
ax.xaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.4)
ax.yaxis.grid(False)

ax.xaxis.set_major_formatter(EngFormatter(unit=""))
[ax.spines[x].set_visible(False) for x in ["top", "right"]]

plt.tight_layout()
```

```python
low_rain = 300

fig, ax = plt.subplots(figsize=(10, 10), dpi=200)

df_plot[df_plot[f"exp_{low_rain}_mm"] > 0].sort_values(
    f"exp_{low_rain}_mm"
).plot.barh(
    x="adm_label",
    y=f"exp_{low_rain}_mm",
    ax=ax,
    color="dodgerblue",
)

ax.legend(
    [f"précip. ≥ {low_rain} mm"],
    title="Population exposée",
)

ax.set_xlabel("Population exposée")
ax.set_ylabel("Arrondissement")
ax.set_title(
    "Haiti : population exposée aux conditions modérées d'ouragan Melissa"
)

# Fine vertical gridlines for readability
ax.xaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.4)
ax.yaxis.grid(False)

# Format population axis nicely
ax.xaxis.set_major_formatter(EngFormatter(unit=""))

# Hide unneeded spines
[ax.spines[x].set_visible(False) for x in ["top", "right"]]

plt.tight_layout()
```

```python

```
