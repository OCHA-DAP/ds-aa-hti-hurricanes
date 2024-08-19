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
import os

import pandas as pd
import pytz

from src.monitoring import monitoring_utils
from src.email import update_emails, utils
from src.utils import blob
from src.constants import *
from src.email import plotting
```

```python
df_monitoring = monitoring_utils.load_existing_monitoring_points("fcast")
```

```python
df_monitoring.iloc[-20:]
```

```python
update_emails.update_fcast_info_emails(verbose=True)
```

```python
update_emails.update_obsv_info_emails()
```

```python
MIN_EMAIL_DISTANCE = 1000
```

```python
df_existing_email_record = utils.load_email_record()
df_existing_email_record
```

```python
MONITOR_ID_TO_DROP = "al042024_fcast_2024-08-08T15:00:00"
```

```python
df_existing_email_record = df_existing_email_record[
    df_existing_email_record["monitor_id"] != MONITOR_ID_TO_DROP
]
df_existing_email_record
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/email/email_record.csv"
blob.upload_csv_to_blob(blob_name, df_existing_email_record)
```

```python

```
