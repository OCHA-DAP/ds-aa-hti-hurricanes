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

# CHIRPS GEFS monitoring

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import datetime

import pandas as pd
from tqdm.notebook import tqdm

from src.utils import blob
from src.datasources import chirps_gefs, codab
```

```python
historical_df = chirps_gefs.load_chirps_gefs_mean_daily()
```

```python
historical_df
```

```python
chirps_gefs.download_recent_chirps_gefs()
```

```python
chirps_gefs.process_recent_chirps_gefs()
```
