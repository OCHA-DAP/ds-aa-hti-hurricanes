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

# Email testing

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import traceback

import pandas as pd
import pytz

from src.monitoring import monitoring_utils
from src.email import email_utils
from src.utils import blob
from src.constants import *
from src.email import plotting
```

```python
MIN_EMAIL_DISTANCE = 1000
```

```python
df_monitoring = monitoring_utils.load_existing_monitoring_points(
    fcast_obsv="fcast"
)
```

```python
df_monitoring = df_monitoring[
    df_monitoring["monitor_id"].str.contains("fcast")
]
```

```python
df_monitoring
```

```python
# df_monitoring = df_monitoring[
#     ~df_monitoring["monitor_id"].str.startswith("TEST")
# ]
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/monitoring/hti_fcast_monitoring.parquet"
blob.upload_parquet_to_blob(blob_name, df_monitoring)
```

```python
df_existing_email_record = email_utils.load_email_record()
```

```python
df_existing_email_record
```

```python
df_existing_email_record = df_existing_email_record[
    df_existing_email_record["atcf_id"] == "TEST_ATCF_ID"
]
```

```python
df_existing_email_record
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
blob.upload_csv_to_blob(blob_name, df_existing_email_record)
```

```python
# remove last line for testing
df_existing_email_record = df_existing_email_record.iloc[:-1]
display(df_existing_email_record)

blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
blob.upload_csv_to_blob(blob_name, df_existing_email_record)
```

```python
# plotting

from

df_monitoring = monitoring_utils.load_existing_monitoring_points("fcast")
if TEST_STORM:
    df_monitoring = add_test_row_to_monitoring(df_monitoring, "fcast")
monitoring_point = df_monitoring.set_index("monitor_id").loc[monitor_id]
haiti_tz = pytz.timezone("America/Port-au-Prince")
cyclone_name = monitoring_point["name"]
atcf_id = monitoring_point["atcf_id"]
issue_time = monitoring_point["issue_time"]
print(type(issue_time))
issue_time_hti = issue_time.astimezone(haiti_tz)
pub_time = issue_time_hti.strftime("%Hh%M")
pub_date = issue_time_hti.strftime("%-d %b %Y")

df_tracks = nhc.load_recent_glb_forecasts()
print(df_tracks.dtypes)
df_single_track = df_tracks[
    (df_tracks["id"] == atcf_id)
    & (df_tracks["issuance"] == issue_time)
]
print(df_single_track)
```
