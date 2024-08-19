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

# Plotting testing

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import io
import base64
from typing import Literal

import pytz
import json

import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go

from src.datasources import nhc, codab
from src.email.utils import (
    TEST_STORM,
    TEST_FCAST_MONITOR_ID,
    TEST_OBSV_MONITOR_ID,
    add_test_row_to_monitoring,
    open_static_image,
)
from src.email.plotting import get_plot_blob_name, convert_datetime_to_fr_str
from src.monitoring import monitoring_utils
from src.utils import blob
from src.constants import *
from src.email import plotting
```

```python
plotting.update_plots("obsv", verbose=True, clobber=["scatter"])
```

```python
plotting.create_scatter_plot(TEST_OBSV_MONITOR_ID, fcast_obsv="obsv")
# plotting.create_map_plot(TEST_OBSV_MONITOR_ID, fcast_obsv="obsv")
# plotting.create_scatter_plot(TEST_FCAST_MONITOR_ID, fcast_obsv="fcast")
# plotting.create_map_plot(TEST_FCAST_MONITOR_ID, fcast_obsv="fcast")
```

```python
fcast_obsv = "obsv"
monitor_id = TEST_OBSV_MONITOR_ID
```

```python
THRESHS
```

```python
df_monitoring = monitoring_utils.load_existing_monitoring_points(fcast_obsv)
if monitor_id in [TEST_FCAST_MONITOR_ID, TEST_OBSV_MONITOR_ID]:
    df_monitoring = add_test_row_to_monitoring(df_monitoring, fcast_obsv)
monitoring_point = df_monitoring.set_index("monitor_id").loc[monitor_id]
haiti_tz = pytz.timezone("America/Port-au-Prince")
cyclone_name = monitoring_point["name"]
issue_time = monitoring_point["issue_time"]
issue_time_hti = issue_time.astimezone(haiti_tz)
blob_name = f"{blob.PROJECT_PREFIX}/processed/stats_{D_THRESH}km.csv"
stats = blob.load_csv_from_blob(blob_name)
if fcast_obsv == "fcast":
    rain_plot_var = "readiness_p"
    s_plot_var = "readiness_s"
    rain_col = "max_roll2_sum_rain"
    rain_source_str = "CHIRPS"
    rain_ymax = 100
    s_thresh = THRESHS["readiness"]["s"]
    rain_thresh = THRESHS["readiness"]["p"]
    fcast_obsv_fr = "prévisions"
    no_pass_text = "pas prévu de passer"
else:
    rain_plot_var = "obsv_p"
    s_plot_var = "obsv_s"
    rain_col = "max_roll2_sum_rain_imerg"
    rain_source_str = "IMERG"
    rain_ymax = 170
    s_thresh = THRESHS["obsv"]["s"]
    rain_thresh = THRESHS["obsv"]["p"]
    fcast_obsv_fr = "observations"
    no_pass_text = "n'a pas passé"


def sid_color(sid):
    color = "blue"
    if sid in CERF_SIDS:
        color = "red"
    return color


stats["marker_size"] = stats["affected_population"] / 6e2
stats["marker_size"] = stats["marker_size"].fillna(1)
stats["color"] = stats["sid"].apply(sid_color)
current_p = monitoring_point[rain_plot_var]
current_s = monitoring_point[s_plot_var]
issue_time_str_fr = convert_datetime_to_fr_str(issue_time_hti)

date_str = (
    f"Prévision "
    f'{monitoring_point["issue_time"].strftime("%Hh%M %d %b UTC")}'
)

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

# ax.scatter(
#     [current_s],
#     [current_p],
#     marker="x",
#     color=CHD_GREEN,
#     linewidths=3,
#     s=100,
# )
# ax.annotate(
#     f"   {cyclone_name}\n\n",
#     (current_s, current_p),
#     va="center",
#     ha="left",
#     color=CHD_GREEN,
#     fontweight="bold",
# )
# ax.annotate(
#     f"\n   {fcast_obsv_fr} émises" f"\n   {issue_time_str_fr}",
#     (current_s, current_p),
#     va="center",
#     ha="left",
#     color=CHD_GREEN,
#     fontstyle="italic",
# )

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

if monitoring_point["min_dist"] >= D_THRESH:
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
    ax.text(
        0.5,
        0.5,
        f"{cyclone_name} {no_pass_text}\n"
        f"à moins de {D_THRESH} km de Haïti",
        fontsize=30,
        color="grey",
        ha="center",
        va="center",
        transform=ax.transAxes,
    )
```

```python

```
