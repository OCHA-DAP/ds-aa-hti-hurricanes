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

# Exposure plotting

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import re

import ocha_stratus as stratus
import matplotlib.pyplot as plt
from matplotlib.ticker import EngFormatter
from matplotlib.colors import BoundaryNorm, ListedColormap

from src.utils import plotting
from src.datasources import codab
from src.constants import *
from src.utils.blob import PROJECT_PREFIX
```

```python
blob_name = (
    f"{PROJECT_PREFIX}/processed/hti_melissa_adm2_wind_rain_exposure.parquet"
)
df_exp = stratus.load_parquet_from_blob(blob_name)
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
adm3 = codab.load_codab_from_blob(admin_level=3)
```

```python
gdf_adm2 = adm2.merge(df_exp, how="outer")
```

```python
df_exp_wind = df_exp.melt(
    id_vars=["ADM2_PCODE"],
    value_vars=[f"exp_{x}_knots" for x in [34, 50, 64]],
    var_name="buffer_speed",
    value_name="pop_exposed",
)
df_exp_wind["buffer_speed"] = (
    df_exp_wind["buffer_speed"]
    .apply(lambda x: x.removeprefix("exp_").removesuffix("_knots"))
    .astype(int)
)
df_exp_wind
```

```python
gdf_template = plotting.build_circle_template(
    gdf_adm2, id_col="ADM2_PCODE", pop_col="total_pop", area_per_person=2_500
)
plotting.plot_template_circles(gdf_template)
```

```python
gdf_plot = gdf_template.merge(adm2[["ADM2_PCODE", "ADM2_FR"]])
```

```python
gdf_plot["adm_label"] = gdf_plot["ADM2_FR"].apply(plotting.wrap_text)
```

```python
gdf_plot
```

```python
fig, ax = plotting.plot_bullseye_exposures(
    gdf_plot,
    df_exp_wind,
    id_col="ADM2_PCODE",
    label_col="adm_label",
    max_font=12,
    min_font=4,
    legend_title="Population exposée\naux vents (taille de bulle\nproportionnelle à population)",
)
ax.set_title(
    "Haïti : population exposée aux vents de Melissa par arrondissement\n\n"
)
```

```python
df_exp_rain = df_exp.melt(
    id_vars=["ADM2_PCODE"],
    value_vars=[f"exp_{x}_mm" for x in [100, 200, 300, 400, 500]],
    var_name="rain_mm",
    value_name="pop_exposed",
)
df_exp_rain["rain_mm"] = (
    df_exp_rain["rain_mm"]
    .apply(lambda x: x.removeprefix("exp_").removesuffix("_mm"))
    .astype(int)
)
df_exp_rain
```

```python
levels = [
    # 25,
    # 50,
    100,
    # 150,
    200,
    300,
    400,
    500,
    # 750,
]
colors = [
    # "lawngreen",
    # "limegreen",
    "yellow",
    # "gold",
    "darkorange",
    "red",
    "firebrick",
    "magenta",
    # "darkmagenta",
]
```

```python
colors_dict = {l: c for l, c in zip(levels, colors)}
```

```python
df_exp_rain[
    (df_exp_rain["rain_mm"] == 100) & (df_exp_rain["pop_exposed"] > 0)
].merge(adm2).sort_values("pop_exposed")
```

```python
fig, ax = plotting.plot_bullseye_exposures(
    gdf_plot,
    df_exp_rain,
    speed_col="rain_mm",
    speeds_order=levels,
    id_col="ADM2_PCODE",
    label_col="adm_label",
    colors=colors_dict,
    max_font=12,
    min_font=4,
    legend_title="Population exposée\naux vents (taille de bulle\nproportionnelle à population)",
    legend_label_fmt="{spd} mm",
)
ax.set_title(
    "Haiti : population exposée aux précip. de Melissa par arrondissement\n"
    "Précip. totales sur 2025-10-21 à 2025-10-29"
)
```

```python
blob_name = "ds-flood-gfm/processed/HTI_adm3_pop_exposure.csv"
df_exp_flood = stratus.load_csv_from_blob(blob_name)
```

```python
df_exp_flood = df_exp_flood.rename(columns={"adm3_src": "ADM3_PCODE"})
```

```python
df_exp_flood = df_exp_flood.merge(
    adm3[["ADM2_PCODE", "ADM2_FR", "ADM3_PCODE", "ADM1_PCODE", "ADM1_FR"]]
)
```

```python
df_exp_flood.groupby(["ADM2_PCODE", "ADM2_FR"]).sum(
    numeric_only=True
).sort_values("chd_gfm_pop_exposed", ascending=False).iloc[:20]
```

```python
df_exp_flood_adm2 = (
    df_exp_flood.groupby(["ADM2_PCODE", "ADM2_FR", "ADM1_PCODE", "ADM1_FR"])
    .sum(numeric_only=True)
    .reset_index()
)
df_exp_flood_adm2
```

```python
df_exp_flood_adm1 = (
    df_exp_flood_adm2.groupby(["ADM1_PCODE", "ADM1_FR"])
    .sum(numeric_only=True)
    .reset_index()
)
```

```python
cutoff = 50
```

```python
df_plot = df_exp_flood.copy()

cols = ["jrc_pop_exposed", "chd_gfm_pop_exposed"]

df_plot["max_cols"] = df_plot[cols].max(axis=1)

df_plot["adm_label"] = (
    df_plot["adm3_name"]
    + " ("
    + df_plot["ADM2_FR"]
    + ", "
    + df_plot["ADM1_FR"]
    + ")"
)

fig, ax = plt.subplots(figsize=(10, 10), dpi=200)

df_plot[df_plot["max_cols"] >= cutoff].sort_values("max_cols").plot.barh(
    x="adm_label",  # label (categorical axis)
    y=cols,  # numeric columns
    ax=ax,
    color=["chocolate", CHD_GREEN],
)

ax.legend(["JRC", "CDH"], title="Méthode")
ax.set_xlabel("Population exposée")
ax.set_ylabel("Commune")

ax.set_title("Haiti : population exposée aux inondations d'ouragan Melissa\n")
ax.text(
    0.5,
    1.01,
    f"Seulement communes avec exposition ≥ {cutoff} personnes indiquées",
    transform=ax.transAxes,
    ha="center",
    style="italic",
    color="grey",
)

ax.xaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
ax.yaxis.grid(False)

ax.xaxis.set_major_formatter(EngFormatter(unit=""))
[ax.spines[x].set_visible(False) for x in ["top", "right"]]
```

```python
df_plot = df_exp_flood_adm2.copy()

df_plot["adm_label"] = df_plot["ADM2_FR"] + " (" + df_plot["ADM1_FR"] + ")"

cols = ["jrc_pop_exposed", "chd_gfm_pop_exposed"]

df_plot["max_cols"] = df_plot[cols].max(axis=1)
df_plot["min_cols"] = df_plot[cols].max(axis=1)

fig, ax = plt.subplots(figsize=(10, 10), dpi=200)

df_plot[df_plot["max_cols"] >= cutoff].sort_values("max_cols").plot.barh(
    x="adm_label",  # label (categorical axis)
    y=cols,  # numeric columns
    ax=ax,
    color=["chocolate", CHD_GREEN],
)

ax.legend(["JRC", "CDH"], title="Méthode")
ax.set_xlabel("Population exposée")
ax.set_ylabel("Arrondissement")

ax.set_title("Haiti : population exposée aux inondations d'ouragan Melissa\n")
ax.text(
    0.5,
    1.01,
    f"Seulement arrondissements avec exposition ≥ {cutoff} personnes indiquées",
    transform=ax.transAxes,
    ha="center",
    style="italic",
    color="grey",
)

ax.xaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
ax.yaxis.grid(False)

ax.xaxis.set_major_formatter(EngFormatter(unit=""))
[ax.spines[x].set_visible(False) for x in ["top", "right"]]
```

```python
df_plot = df_exp_flood_adm1.copy()

df_plot["adm_label"] = df_plot["ADM1_FR"]

cols = ["jrc_pop_exposed", "chd_gfm_pop_exposed"]

df_plot["max_cols"] = df_plot[cols].max(axis=1)
df_plot["min_cols"] = df_plot[cols].max(axis=1)

fig, ax = plt.subplots(figsize=(10, 10), dpi=200)

df_plot[df_plot["max_cols"] >= cutoff].sort_values("max_cols").plot.barh(
    x="adm_label",  # label (categorical axis)
    y=cols,  # numeric columns
    ax=ax,
    color=["chocolate", CHD_GREEN],
)

ax.legend(["JRC", "CDH"], title="Méthode")
ax.set_xlabel("Population exposée")
ax.set_ylabel("Arrondissement")

ax.set_title("Haiti : population exposée aux inondations d'ouragan Melissa\n")
ax.text(
    0.5,
    1.01,
    f"Seulement départements avec exposition ≥ {cutoff} personnes indiquées",
    transform=ax.transAxes,
    ha="center",
    style="italic",
    color="grey",
)

ax.xaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
ax.yaxis.grid(False)

ax.xaxis.set_major_formatter(EngFormatter(unit=""))
[ax.spines[x].set_visible(False) for x in ["top", "right"]]
```

```python
bounds = [10, 50, 100, 200, 300]
colors = [
    "white",
    "gold",
    "darkorange",
    "red",
    "magenta",
    "darkmagenta",
]  # light→dark blues
cmap = ListedColormap(colors)
# cmap.set_over("crimson")  # color for values >
# cmap.set_under("lightgray")
norm = BoundaryNorm(bounds, cmap.N, extend="both")
```

```python
gdf_plot = adm3.merge(df_exp_flood)
gdf_plot["plot_flood_exposure"] = gdf_plot[
    ["jrc_pop_exposed", "chd_gfm_pop_exposed"]
].max(axis=1)

# Plot
fig, ax = plt.subplots(dpi=200, figsize=(12, 6))
gdf_plot.plot(
    ax=ax,
    column="plot_flood_exposure",
    cmap=cmap,
    norm=norm,
    linewidth=0,
)
adm1.boundary.plot(ax=ax, linewidth=0.3, color="k")
adm3.boundary.plot(ax=ax, linewidth=0.1, color="k")

ax.axis("off")
ax.set_title("Exposition aux inondations par commune", fontsize=11)

# Add colorbar with 'extend' arrow
sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
sm._A = []
cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02, extend="max")
cbar.set_label("Population exposée (max. de JRC et CDH)")
cbar.set_ticks(bounds)
```

```python
gdf_plot = adm2.merge(df_exp_flood_adm2)
gdf_plot["plot_flood_exposure"] = gdf_plot[
    ["jrc_pop_exposed", "chd_gfm_pop_exposed"]
].max(axis=1)

gdf_plot["adm_label_break"] = gdf_plot["ADM2_FR"].apply(plotting.wrap_text)
# Plot
fig, ax = plt.subplots(dpi=200, figsize=(12, 6))
gdf_plot.plot(
    ax=ax,
    column="plot_flood_exposure",
    cmap=cmap,
    norm=norm,
    linewidth=0,
)
adm1.boundary.plot(ax=ax, linewidth=0.3, color="k")
adm2.boundary.plot(ax=ax, linewidth=0.1, color="k")

for _, row in gdf_plot.iterrows():
    if row["plot_flood_exposure"] > 0:
        c = row.geometry.centroid
        ax.annotate(
            row["adm_label_break"],
            (c.x, c.y),
            ha="center",
            va="center",
            fontsize=4,
        )

ax.axis("off")
ax.set_title("Exposition aux inondations par arrondissement", fontsize=11)

# Add colorbar with 'extend' arrow
sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
sm._A = []
cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02, extend="max")
cbar.set_label("Population exposée (max. de JRC et CDH)")
cbar.set_ticks(bounds)
```

```python
gdf_plot = adm1.merge(df_exp_flood_adm1)
gdf_plot["plot_flood_exposure"] = gdf_plot[
    ["jrc_pop_exposed", "chd_gfm_pop_exposed"]
].max(axis=1)

gdf_plot["adm_label_break"] = gdf_plot["ADM1_FR"].apply(plotting.wrap_text)
# Plot
fig, ax = plt.subplots(dpi=200, figsize=(12, 6))
gdf_plot.plot(
    ax=ax,
    column="plot_flood_exposure",
    cmap=cmap,
    norm=norm,
    linewidth=0,
)
adm1.boundary.plot(ax=ax, linewidth=0.3, color="k")

for _, row in gdf_plot.iterrows():
    c = row.geometry.centroid
    ax.annotate(
        row["adm_label_break"],
        (c.x, c.y),
        ha="center",
        va="center",
        fontsize=12,
    )

ax.axis("off")
ax.set_title("Exposition aux inondations par département", fontsize=11)

# Add colorbar with 'extend' arrow
sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
sm._A = []
cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02, extend="max")
cbar.set_label("Population exposée (max. de JRC et CDH)")
cbar.set_ticks(bounds)
```

```python
df_out = (
    adm3[
        [
            "ADM1_PCODE",
            "ADM1_FR",
            "ADM2_PCODE",
            "ADM2_FR",
            "ADM3_PCODE",
            "ADM3_FR",
        ]
    ]
    .merge(df_exp_flood)
    .sort_values(["ADM1_FR", "ADM2_FR", "ADM3_FR"])
)
```

```python
save_path = "temp/hti_melissa_adm3_flood_exposure.csv"
df_out.to_csv(save_path, index=False, encoding="utf-8-sig")

save_path = "temp/hti_melissa_adm3_flood_exposure.xlsx"
df_out.to_excel(save_path, index=False)
```

```python

```
