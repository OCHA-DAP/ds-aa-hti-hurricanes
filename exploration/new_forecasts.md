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

# New forecasts

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
# create dummy forecast
```

```python
import pandas as pd
from io import BytesIO

from src.utils import blob
from src.datasources import nhc
from src.constants import *
```

```python
df = nhc.load_processed_historical_forecasts()
```

```python
df_matthew = df[df["atcf_id"] == MATTHEW_ATCF_ID]
df_matthew = df_matthew.rename(
    columns={
        "atcf_id": "id",
        "issue_time": "issuance",
        "valid_time": "validTime",
        "windspeed": "maxwind",
        "lat": "latitude",
        "lon": "longitude",
    }
).drop(columns=["pressure"])
df_matthew["name"] = "TEST"
df_matthew["basin"] = "al"
df_matthew
```

```python
df_current = pd.read_csv(
    BytesIO(
        blob.dev_glb_container_client.get_blob_client(
            "noaa/nhc/forecasted_tracks.csv"
        )
        .download_blob()
        .readall()
    ),
    sep=";",
    parse_dates=["issuance", "validTime"],
)
df_current
```

```python
df_out = pd.merge(df_matthew, df_current, how="outer")
```

```python
df_out
```

```python
blob.dev_glb_container_client.get_blob_client(
    "noaa/nhc/forecasted_tracks.csv"
).upload_blob(df_out.to_csv(index=False, sep=";"), overwrite=True)
```
