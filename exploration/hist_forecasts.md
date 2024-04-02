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

# Historical forecasts

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import os
import gzip
from ftplib import FTP
from pathlib import Path
from io import StringIO, BytesIO

import fiona
import requests
import geopandas as gpd
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm

from src.utils import blob
from src.datasources import nhc
```

```python
# nhc.download_archive_forecasts()
```

```python
blob.list_container_blobs()
```

```python
DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
NHC_RAW_DIR = DATA_DIR / "public" / "raw" / "hti" / "nhc"
```
