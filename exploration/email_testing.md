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
df_monitoring_test = df_monitoring[
    df_monitoring["monitor_id"] == "al022024_fcast_2024-07-01T15:00:00"
].copy()
df_monitoring_test[
    ["monitor_id", "name", "atcf_id", "readiness_trigger", "action_trigger"]
] = (
    "TEST_MONITOR_ID",
    "TEST_STORM_NAME",
    "TEST_ATCF_ID",
    True,
    True,
)
df_monitoring = pd.concat(
    [df_monitoring, df_monitoring_test], ignore_index=True
)
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
# remove last line for testing
df_existing_email_record = df_existing_email_record.iloc[:-1]
```

```python
df_existing_email_record
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
blob.upload_csv_to_blob(blob_name, df_existing_email_record)
```

```python
# trigger email

dicts = []
for atcf_id, group in df_monitoring.groupby("atcf_id"):
    for trigger_name in ["readiness", "action"]:
        if (
            atcf_id
            in df_existing_email_record[
                df_existing_email_record["email_type"] == trigger_name
            ]["atcf_id"].unique()
        ):
            print(f"already sent {trigger_name} email for {atcf_id}")
        else:
            for (
                monitor_id,
                row,
            ) in group.set_index("monitor_id").iterrows():
                if row[f"{trigger_name}_trigger"] and not row["past_cutoff"]:
                    try:
                        print(f"sending {trigger_name} email for {monitor_id}")
                        email_utils.send_trigger_email(
                            monitor_id=monitor_id,
                            trigger_name=trigger_name,
                        )
                        dicts.append(
                            {
                                "monitor_id": monitor_id,
                                "atcf_id": atcf_id,
                                "email_type": trigger_name,
                            }
                        )
                    except Exception as e:
                        print(f"could not send email for {monitor_id}: {e}")
                        traceback.print_exc()
                        pass

df_new_email_record = pd.DataFrame(dicts)
df_combined_email_record = pd.concat(
    [df_existing_email_record, df_new_email_record], ignore_index=True
)
```

```python
df_combined_email_record
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
blob.upload_csv_to_blob(blob_name, df_combined_email_record)
```

```python

```
