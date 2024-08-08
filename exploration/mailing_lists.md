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

# Mailing lists

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from src.utils import blob
```

## Test list

```python
df = pd.DataFrame(
    columns=["email", "name", "trigger", "info"],
    data=[
        ["tristan.downing@un.org", "TEST_NAME", "to", "to"]
        ["downing.tristan@gmail.com", "TEST_NAME", "to", Non],
    ],
)
blob_name = f"{blob.PROJECT_PREFIX}/email/test_distribution_list.csv"
blob.upload_csv_to_blob(blob_name, df)
df
```

## Actual list

```python
df = pd.DataFrame(
    columns=["email", "name", "trigger", "info"],
    data=[
        ["regina.omlor@un.org", "Regina Omlor", "to", "to"],
        ["silvia.pieretto@wfp.org", "Silvia Pieretto", "to", "to"],
        ["daniel.ham@wfp.org", "Daniel Ham", "to", "to"],
        ["clement.rouquette@wfp.org", "Clement Rouquette", "to", "to"],
        ["erwan.rumen@wfp.org", "Erwan Ruman", "to", None],
        ["tristan.downing@un.org", "Tristan Downing", "cc", "cc"],
    ],
)
df
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/email/distribution_list.csv"
blob.upload_csv_to_blob(blob_name, df)
df
```

```python

```
