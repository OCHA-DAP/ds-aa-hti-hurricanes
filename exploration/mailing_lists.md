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
from src.email.utils import is_valid_email
```

## Test list

```python
df_test = pd.DataFrame(
    columns=["email", "name", "trigger", "info"],
    data=[
        ["tristan.downing@un.org", "TEST_NAME", "to", "to"],
        ["downing.tristan@gmail.com", "TEST_NAME", "to", None],
    ],
)
print("invalid emails: ")
display(df_test[~df_test["email"].apply(is_valid_email)])
blob_name = f"{blob.PROJECT_PREFIX}/email/test_distribution_list.csv"
blob.upload_csv_to_blob(blob_name, df_test)
df_test
```

## Actual list

```python
df_actual = pd.DataFrame(
    columns=["name", "email", "trigger", "info"],
    data=[
        # HC
        [
            "Ingeborg Ulrika Ulfsdotter Richardson",
            "ulrika.richardson@un.org",
            "to",
            None,
        ],
        # OCHA HTI
        ["Abdoulaye Sawadogo", "sawadogoa@un.org", "to", None],
        ["Emmanuelle Schneider", "schneider1@un.org", "cc", "to"],
        ["Shedna Italis", "shedna.italis@un.org", "cc", "to"],
        # OCHA HQ
        ["Michael Jensen", "jensen7@un.org", "cc", None],
        ["Daniel Pfister", "pfisterd@un.org", "cc", None],
        ["Regina Omlor", "regina.omlor@un.org", "cc", "to"],
        ["Nicolas Rost", "rostn@un.org", "cc", "to"],
        ["Julia Wittig", "wittigj@un.org", "cc", "to"],
        ["Yakubu Alhassan", "yakubu.alhassan@un.org", "cc", "to"],
        ["Jacopo Damelio", "jacopo.damelio@un.org", "cc", "to"],
        ["OCHA-cerf", "cerf@un.org", "cc", None],
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
        # CHD DS
        ["Tristan Downing", "tristan.downing@un.org", "cc", "cc"],
        ["Zachary Arno", "zachary.arno@un.org", "cc", "cc"],
        ["Pauline Ndirangu", "pauline.ndirangu@un.org", "cc", "cc"],
    ],
)
print("invalid emails: ")
display(df_actual[~df_actual["email"].apply(is_valid_email)])
df_actual
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/email/distribution_list.csv"
blob.upload_csv_to_blob(blob_name, df_actual)
df_actual
```

```python

```
