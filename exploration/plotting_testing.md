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
import plotly.graph_objects as go

from src.datasources import nhc, codab
from src.email.utils import (
    TEST_STORM,
    TEST_MONITOR_ID,
    add_test_row_to_monitoring,
    open_static_image,
)
from src.email.plotting import get_plot_blob_name
from src.monitoring import monitoring_utils
from src.utils import blob
from src.constants import *
```

```python
def convert_datetime_to_fr_str(x: pd.Timestamp) -> str:
    fr_str = x.strftime("%Hh%M, %-d %b")
    for en_mo, fr_mo in FRENCH_MONTHS.items():
        fr_str = fr_str.replace(en_mo, fr_mo)
    return fr_str
```

```python
update_fcast_plots(verbose=True, clobber=["scatter"])
```

```python
adm = codab.load_codab_from_blob(admin_level=0)
```

```python
trig_zone = codab.load_buffer()
```

```python
lts = {
    "action": {
        "color": "darkorange",
        "plot_color": "black",
        "dash": "solid",
        "label": "Action",
        "zorder": 2,
        "lt_max": pd.Timedelta(days=3),
        "lt_min": pd.Timedelta(days=-1),
        "threshs": {
            "roll2_rain_dist": 42,
            "wind_dist": 64,
            "dist_min": 230,
        },
    },
    "readiness": {
        "color": "dodgerblue",
        "plot_color": "grey",
        "dash": "dot",
        "label": "Mobilisation",
        "zorder": 1,
        "lt_max": pd.Timedelta(days=5),
        "lt_min": pd.Timedelta(days=2),
        "threshs": {
            "roll2_rain_dist": 42,
            "wind_dist": 64,
            "dist_min": 230,
        },
    },
    "obsv": {
        "color": "dodgerblue",
        "plot_color": "grey",
        "dash": "dot",
        "label": "Observationnel",
        "zorder": 1,
        "lt_max": pd.Timedelta(days=0),
        "lt_min": pd.Timedelta(days=0),
        "threshs": {
            "roll2_rain_dist": 60,
            "wind_dist": 50,
            "dist_min": 230,
        },
    },
}
```

```python
monitor_id = "TEST_MONITOR_ID"
# monitor_id = "al022024_fcast_2024-06-28T21:00:00"
# monitor_id = "al022024_fcast_2024-07-09T03:00:00"
# monitor_id = "al022024_fcast_2024-07-04T09:00:00"
plot_type = "map"
```

```python
fcast_obsv = "obsv"
```

```python
df_monitoring = monitoring_utils.load_existing_monitoring_points(fcast_obsv)
if monitor_id == TEST_MONITOR_ID:
    df_monitoring = add_test_row_to_monitoring(df_monitoring, fcast_obsv)
monitoring_point = df_monitoring.set_index("monitor_id").loc[monitor_id]
haiti_tz = pytz.timezone("America/Port-au-Prince")
cyclone_name = monitoring_point["name"]
atcf_id = monitoring_point["atcf_id"]
if atcf_id == "TEST_ATCF_ID":
    atcf_id = "al022024"
issue_time = monitoring_point["issue_time"]
issue_time_hti = issue_time.astimezone(haiti_tz)
```

```python
monitoring_point
```

```python
df_tracks = nhc.load_recent_glb_obsv()
if fcast_obsv == "fcast":
    tracks_f = df_tracks[
        (df_tracks["id"] == atcf_id) & (df_tracks["issuance"] == issue_time)
    ].copy()
else:
    tracks_f = df_tracks[
        (df_tracks["id"] == atcf_id) & (df_tracks["lastUpdate"] <= issue_time)
    ].copy()
    tracks_f = tracks_f.rename(
        columns={"lastUpdate": "validTime", "intensity": "maxwind"}
    )
    tracks_f["issuance"] = tracks_f["validTime"]
tracks_f["validTime_hti"] = tracks_f["validTime"].apply(
    lambda x: x.astimezone(haiti_tz)
)
tracks_f["valid_time_str"] = tracks_f["validTime_hti"].apply(
    convert_datetime_to_fr_str
)
```

```python
tracks_f
```

```python
# if fcast_obsv == "fcast":
tracks_f["lt"] = tracks_f["validTime"] - tracks_f["issuance"]
rain_plot_var = "readiness_p" if fcast_obsv == "fcast" else "obsv_p"
rain_level = monitoring_point[rain_plot_var]
```

```python
fig = go.Figure()
for geom in adm.geometry[0].geoms:
    x, y = geom.exterior.coords.xy
    fig.add_trace(
        go.Scattermapbox(
            lon=list(x),
            lat=list(y),
            mode="lines",
            line_color="grey",
            showlegend=False,
        )
    )

fig.add_trace(
    go.Choroplethmapbox(
        geojson=json.loads(trig_zone.geometry.to_json()),
        locations=trig_zone.index,
        z=[1],
        colorscale="Reds",
        marker_opacity=0.2,
        showscale=False,
        marker_line_width=0,
        hoverinfo="none",
    )
)

relevant_lts = ["readiness", "action"] if fcast_obsv == "fcast" else ["obsv"]

for lt_name in relevant_lts:
    lt_params = lts[lt_name]
    if lt_name == "obsv":
        dff = tracks_f.copy()
    else:
        dff = tracks_f[
            (tracks_f["lt"] <= lt_params["lt_max"])
            & (tracks_f["lt"] >= lt_params["lt_min"])
        ]
    # triggered points
    dff_trig = dff[
        (dff["maxwind"] >= lt_params["threshs"]["wind_dist"])
        & (dff["lt"] >= lt_params["lt_min"])
    ]
    fig.add_trace(
        go.Scattermapbox(
            lon=dff_trig["longitude"],
            lat=dff_trig["latitude"],
            mode="markers",
            marker=dict(size=50, color="red"),
        )
    )
    # all points
    fig.add_trace(
        go.Scattermapbox(
            lon=dff["longitude"],
            lat=dff["latitude"],
            mode="markers+text+lines",
            marker=dict(size=40, color=lt_params["plot_color"]),
            text=dff["maxwind"].astype(str),
            line=dict(width=2, color=lt_params["plot_color"]),
            textfont=dict(size=20, color="white"),
            customdata=dff["valid_time_str"],
            hovertemplate=("Heure valide: %{customdata}<extra></extra>"),
        )
    )

encoded_legend = open_static_image("map_legend.png")
fig.update_layout(
    # title=plot_title,
    mapbox_style="open-street-map",
    # mapbox_zoom=zoom,
    # mapbox_center_lat=(lat_max + lat_min) / 2,
    # mapbox_center_lon=(lon_max + lon_min) / 2,
    margin={"r": 0, "t": 50, "l": 0, "b": 0},
    height=850,
    width=800,
    showlegend=False,
    images=[
        dict(
            source=f"data:image/png;base64,{encoded_legend}",
            xref="paper",
            yref="paper",
            x=0.01,
            y=0.01,
            sizex=0.3,
            sizey=0.3,
            xanchor="left",
            yanchor="bottom",
            opacity=0.7,
        )
    ],
)
fig.show()
```

```python

```

```python
buffer = io.BytesIO()
# scale corresponds to 150 dpi
fig.write_image(buffer, format="png", scale=2.08)
buffer.seek(0)

blob_name = get_blob_name(monitor_id, plot_type)
blob.upload_blob_data(blob_name, buffer)
```

```python

```
