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

# IMERG COG

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import tempfile
from io import BytesIO

import pandas as pd
import rioxarray as rxr

from src.datasources import imerg, codab
from src.utils import blob

from tqdm.notebook import tqdm
```

```python
existing_files = [
    x.name
    for x in blob.dev_glb_container_client.list_blobs(
        name_starts_with="imerg/v07b"
    )
]
```

```python
existing_files[-10:]
```

```python
existing_files = [x.name for x in blob.dev_glb_container_client.list_blobs(name_starts_with="imerg/v07b")]

for date in tqdm(pd.date_range("2008-02-23", "2020-01-19")):
    output_path = f"imerg/v07b/imerg-daily-late-{date.date()}.tif"
    if output_path in existing_files:
        print(f"{output_path} already exists")
        continue
    imerg.download_imerg(date)
    da_in = imerg.process_imerg()
    da_in = da_in.rename({"lon": "x", "lat": "y"}).squeeze(drop=True)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".tif") as tmpfile:
        temp_filename = tmpfile.name
        da_in.rio.to_raster(temp_filename, driver="COG")
        with open(temp_filename, "rb") as f:
            blob.dev_glb_container_client.get_blob_client(output_path).upload_blob(
                f, overwrite=True
            )
```

```python
output_path
```

```python
input_path = "imerg/v07b/imerg-daily-late-2003-05-17.tif"
data = (
    blob.dev_glb_container_client.get_blob_client(input_path)
    .download_blob()
    .readall()
)
```

```python
test = rxr.open_rasterio(BytesIO(data))
```

```python
test.where(test > 0).plot()
```
