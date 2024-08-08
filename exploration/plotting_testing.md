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
    TEST_FCAST_MONITOR_ID,
    TEST_OBSV_MONITOR_ID,
    add_test_row_to_monitoring,
    open_static_image,
)
from src.email.plotting import get_plot_blob_name
from src.monitoring import monitoring_utils
from src.utils import blob
from src.constants import *
from src.email import plotting
```

```python
plotting.update_plots("obsv", verbose=True, clobber=["map"])
```

```python
plotting.create_scatter_plot(TEST_OBSV_MONITOR_ID, fcast_obsv="obsv")
```
