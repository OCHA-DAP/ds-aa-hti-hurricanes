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

# Forecast scatter plot

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

from src.utils import blob
from src.datasources import ibtracs
```

```python
D_THRESH = 230
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/stats_{D_THRESH}km.csv"
stats = blob.load_csv_from_blob(blob_name)
```

```python
stats
```

```python
rain_col = "max_roll2_sum_rain"
current_p = 7.958333
current_s = 135
current_name = "Beryl"
CHD_GREEN = "#1bb580"


def sid_color(sid):
    color = "blue"
    if sid in ibtracs.CERF_SIDS:
        color = "red"
    # elif sid in ibtracs.IMPACT_SIDS:
    #     color = "orange"
    return color


stats["marker_size"] = stats["affected_population"] / 6e2
stats["marker_size"] = stats["marker_size"].fillna(1)
stats["color"] = stats["sid"].apply(sid_color)

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
    f"\n   Prévision 2 juillet 15h00 UTC",
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
    (155, 100),
    ha="right",
    va="top",
    color="orange",
    fontweight="bold",
)
ax.annotate(
    "\n\nAllocations CERF en rouge   ",
    (155, 100),
    ha="right",
    va="top",
    color="crimson",
    fontstyle="italic",
)

ax.set_xlim(right=155, left=0)
ax.set_ylim(top=100, bottom=0)

ax.set_xlabel("Vitesse de vent maximum (noeuds)")
ax.set_ylabel(
    "Précipitations pendant deux jours consécutifs maximum, "
    "moyenne sur toute la superficie (mm)"
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
