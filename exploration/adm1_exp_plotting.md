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

# Exposure by department

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

from src.utils import plotting
from src.datasources import codab
from src.constants import *
from src.utils.blob import PROJECT_PREFIX
```

```python
adm0 = codab.load_codab_from_blob(admin_level=0)
adm1 = codab.load_codab_from_blob(admin_level=1)
adm2 = codab.load_codab_from_blob(admin_level=2)
adm3 = codab.load_codab_from_blob(admin_level=3)
```

```python
blob_name = (
    f"{PROJECT_PREFIX}/processed/hti_melissa_adm2_wind_rain_exposure.parquet"
)
df_exp = stratus.load_parquet_from_blob(blob_name)
```

```python
blob_name = "ds-flood-gfm/processed/HTI_adm3_pop_exposure.csv"
df_exp_flood = stratus.load_csv_from_blob(blob_name)
df_exp_flood = df_exp_flood.rename(columns={"adm3_src": "ADM3_PCODE"})
```

```python
df_exp_flood = df_exp_flood.merge(
    adm3[["ADM2_PCODE", "ADM2_FR", "ADM3_PCODE", "ADM1_PCODE", "ADM1_FR"]]
)
```

```python
df_exp_flood.sum()
```

```python
cols = [f"{x}_pop_exposed" for x in ["jrc", "chd_gfm"]]
df_exp_flood_adm2 = (
    df_exp_flood.groupby("ADM2_PCODE")[cols].sum().reset_index()
)
df_exp_flood_adm2[cols] = df_exp_flood_adm2[cols].astype(int)
```

```python
df_exp_flood_adm2
```

```python
df_exp.sum()
```

```python
df_exp_combined = df_exp.merge(df_exp_flood_adm2)
```

```python
df_exp_adm1 = (
    df_exp_combined.groupby("ADM1_PCODE").sum(numeric_only=True).reset_index()
)
```

```python
df_exp_adm1.sum()
```

```python
df_out = (
    adm1[["ADM1_PCODE", "ADM1_FR"]].merge(df_exp_adm1).sort_values("ADM1_FR")
)
df_out
```

```python
save_path = "temp/hti_melissa_adm1_exposure.csv"
df_out.to_csv(save_path, index=False, encoding="utf-8-sig")

save_path = "temp/hti_melissa_adm1_exposure.xlsx"
df_out.to_excel(save_path, index=False)
```

```python
gdf_adm1 = adm1.merge(df_exp_adm1)
```

```python
gdf_template = plotting.build_circle_template(
    gdf_adm1, id_col="ADM1_PCODE", pop_col="total_pop", area_per_person=4_000
)
plotting.plot_template_circles(gdf_template)
```

```python
df_exp_wind = df_exp_adm1.melt(
    id_vars=["ADM1_PCODE"],
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
gdf_plot = gdf_template.merge(adm1[["ADM1_PCODE", "ADM1_FR"]])
gdf_plot["adm_label"] = gdf_plot["ADM1_FR"].apply(plotting.wrap_text)
```

```python
fig, ax = plotting.plot_bullseye_exposures(
    gdf_plot,
    df_exp_wind,
    id_col="ADM1_PCODE",
    label_col="adm_label",
    max_font=20,
    min_font=10,
    legend_title="Population exposée\naux vents (taille de\nbulle proportionnelle\nà population)",
    legend_loc="upper left",
)
ax.set_title(
    "Haïti : population exposée aux vents de Melissa par département\n"
)
```

```python
df_exp_rain = df_exp_adm1.melt(
    id_vars=["ADM1_PCODE"],
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
fig, ax = plotting.plot_bullseye_exposures(
    gdf_plot,
    df_exp_rain,
    speed_col="rain_mm",
    speeds_order=levels,
    id_col="ADM1_PCODE",
    label_col="adm_label",
    colors=colors_dict,
    max_font=20,
    min_font=10,
    legend_title="Population exposée\naux précip. (taille de\nbulle proportionnelle\nà population)",
    legend_label_fmt="{spd} mm",
    legend_loc="upper left",
)
ax.set_title(
    "Haiti : population exposée aux précip. de Melissa par département\n"
    "Précip. totales sur 2025-10-21 à 2025-10-29"
)
```

```python
df_plot = df_exp_adm1.copy().merge(adm1)
cutoff = 50

cols = ["jrc_pop_exposed", "chd_gfm_pop_exposed"]

df_plot["max_cols"] = df_plot[cols].max(axis=1)

fig, ax = plt.subplots(figsize=(10, 7), dpi=200)

df_plot[df_plot["max_cols"] >= cutoff].sort_values("max_cols").plot.barh(
    x="ADM1_FR",  # label (categorical axis)
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
high_rain = 400

max_col = f"max_{high_rain}_mm_34_knots"

df_plot[max_col] = df_plot[[f"exp_{high_rain}_mm", "exp_34_knots"]].max(axis=1)

fig, ax = plt.subplots(figsize=(10, 5), dpi=200)

df_plot[df_plot[max_col] > 0].sort_values(max_col).plot.barh(
    x="ADM1_FR",
    y=[f"exp_{high_rain}_mm", "exp_34_knots"],
    ax=ax,
    color=["dodgerblue", "darkorange"],
)

ax.legend(
    [f"précip. ≥ {high_rain} mm", "vents ≥ 34 nœuds"],
    title="Population exposée",
    loc="lower right",
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

fig, ax = plt.subplots(figsize=(10, 5), dpi=200)

df_plot[df_plot[f"exp_{low_rain}_mm"] > 0].sort_values(
    f"exp_{low_rain}_mm"
).plot.barh(
    x="ADM1_FR",
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
df_exp_adm1.merge(adm1[["ADM1_PCODE", "ADM1_FR"]])
```

```python
df_exp_adm1["dgpc_affected"] = [
    1_009_704,
    112_470,
    23_450,
    0,
    379_276,
    0,
    286_682,
    171_206,
    239_104,
    100_761,
]
```

```python
df_exp_adm1
```

```python
fig, ax = plt.subplots(figsize=(10, 6), dpi=200)
cols = [
    "dgpc_affected",
    "exp_34_knots",
    "exp_300_mm",
    "exp_400_mm",
]
df_exp_adm1.merge(adm1[["ADM1_PCODE", "ADM1_FR"]]).plot.bar(
    x="ADM1_FR", y=cols, ax=ax
)
ax.legend(
    [
        'DGPC "Affectée"',
        "exp. vents ≥ 34 nœuds",
        "exp. précip. ≥ 300 mm",
        "exp. précip. ≥ 400 mm",
    ],
    title="Population",
)
ax.set_xlabel("Département")
ax.set_ylabel("Population")
ax.yaxis.set_major_formatter(EngFormatter(unit=""))
[ax.spines[x].set_visible(False) for x in ["top", "right"]]
ax.set_title('Haïti : population "Affectée" et exposée')
```

```python
df_exp_adm1.corr(numeric_only=True)["dgpc_affected"]
```
