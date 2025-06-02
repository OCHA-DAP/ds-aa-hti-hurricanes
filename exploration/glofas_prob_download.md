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
    display_name: ds-aa-nga-flooding
    language: python
    name: ds-aa-nga-flooding
---

# GloFAS probabilistic downloading

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import datetime

import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from src.datasources import glofas, floodscan
from src.constants import *
```

```python
glofas.download_reforecast_ensembles()
```

```python
[f"{x:02}" for x in range(1, 32)]
```
