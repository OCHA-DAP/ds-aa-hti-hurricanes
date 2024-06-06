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

# CODAB

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import json

import geopandas as gpd
import plotly.graph_objects as go

from src.datasources import codab
```

```python
codab.download_codab_to_blob()
```

```python
adm0 = codab.load_codab_from_blob(admin_level=0)
```

```python
adm0
```

```python
adm0.total_bounds
```

```python

```

```python
codab.process_buffer()
```

```python
buffer = codab.load_buffer()
```

```python
buffer
```

```python
adm
```

```python
adm = codab.load_codab(admin_level=0)
```

```python
adm.plot()
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
            line_color="red",
            showlegend=False,
        )
    )
fig.add_trace(
    go.Choroplethmapbox(
        geojson=json.loads(buffer.geometry.to_json()),
        locations=buffer.index,
        z=[1],
        colorscale="Reds",
        marker_opacity=0.2,
        showscale=False,
        marker_line_width=0,
        hoverinfo="none",
    )
)
fig.update_layout(
    mapbox_style="open-street-map",
    margin={"r": 0, "t": 30, "l": 0, "b": 0},
    title="Zone à moins de 230 km de Haïti",
    mapbox_zoom=5.8,
    mapbox_center_lat=19,
    mapbox_center_lon=-73,
    height=630,
    width=800,
)
fig.show()
```

```python

```
