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
from src.email import update_emails, utils
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
import src.email.utils

df_existing_email_record = src.email.utils.load_email_record()
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
fcast_obsv = "fcast"
monitor_id = "al022024_fcast_2024-06-28T21:00:00"
TEST_STORM = email_utils.TEST_STORM
```

```python
# plotting
import src.email.utils

df_monitoring = monitoring_utils.load_existing_monitoring_points(fcast_obsv)
if TEST_STORM:
    df_monitoring = src.email.utils.add_test_row_to_monitoring(
        df_monitoring, fcast_obsv
    )
print(df_monitoring)
monitoring_point = df_monitoring.set_index("monitor_id").loc[monitor_id]
print(monitoring_point)
```

```python

```
