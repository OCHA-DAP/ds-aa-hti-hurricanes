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
        ["tristan.downing@un.org", "TEST_NAME", "to", "to"],
        ["downing.tristan@gmail.com", "TEST_NAME", "to", None],
    ],
)
blob_name = f"{blob.PROJECT_PREFIX}/email/test_distribution_list.csv"
blob.upload_csv_to_blob(blob_name, df)
df
```

## Actual list

```python
df = pd.DataFrame(
    columns=["name", "email", "trigger", "info"],
    data=[
        # OCHA HTI
        ["Emmanuelle Schneider", "schneider1@un.org", "to", "to"],
        ["Shedna Italis", "shedna.italis@un.org", "to", "to"],
        # OCHA HQ
        ["Regina Omlor", "regina.omlor@un.org", "cc", "to"],
        ["Nicolas Rost", "rostn@un.org", "cc", "to"],
        ["Julia Wittig", "wittigj@un.org", "cc", "to"],
        ["Yakubu Alhassan", "yakubu.alhassan@un.org", "cc", "to"],
        # WFP
        ["Erwan Ruman", "erwan.rumen@wfp.org", "cc", None],
        ["Silvia Pieretto", "silvia.pieretto@wfp.org", "cc", "to"],
        ["Daniel Ham", "daniel.ham@wfp.org", "cc", "to"],
        ["Clement Rouquette", "clement.rouquette@wfp.org", "cc", "to"],
        # UNICEF
        ["Dorica Tasuzgika Phiri", "dtphiri@unicef.org", "cc", "to"],
        ["Boris Matous", "bmatous@unicef.org", "cc", "to"],
        # IOM
        ["Daniele Feibei", "dfebei@iom.int", "cc", "to"],
        # me
        ["Tristan Downing", "tristan.downing@un.org", "cc", "cc"],
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
