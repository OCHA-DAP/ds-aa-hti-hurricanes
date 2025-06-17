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

# Monitoring testing 2025

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import re

from src.monitoring.monitoring_utils import *
```

```python
# extract group that fails
# set code to return group after
# logger.warning(
#     f"Skipping {atcf_id} due to interpolation error: {e}"
# )
# instead of raising error
df_test = update_obsv_monitoring()
```

```python
df_test.sort_values(["lastUpdate", "name"])
```

```python
# see duplicates
df_test[df_test.duplicated(subset=["lastUpdate"], keep=False)]
```

```python
df_test[df_test.duplicated(subset=["lastUpdate", "latitude"], keep=False)]
```

```python
df_test.pivot(columns="name", index="longitude", values="latitude").plot(
    marker="."
)
```

```python
df_test.pivot(columns="name", index="lastUpdate", values="latitude").plot(
    marker="."
)
```

```python
# looks like Four-E is wrong
df_test.pivot(columns="name", index="lastUpdate", values="longitude").plot(
    marker="."
)
```

```python
# make regex to check for numeric name
pattern = re.compile(
    r"\b(?:One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|Eleven|Twelve|Thirteen|Fourteen|Fifteen|Sixteen|Seventeen|Eighteen|Nineteen|Twenty)\b"
)
```

```python
df_test["numeric_name"] = df_test["name"].apply(
    lambda x: bool(pattern.search(x))
)
```

```python
df_test
```

```python
# remove duplicates and take the one that doesn't have a numeric name
df_test.sort_values("numeric_name", ascending=True).drop_duplicates(
    subset=["lastUpdate"]
).sort_values("lastUpdate")
```

```python
df_test.sort_values("numeric_name", ascending=False).drop_duplicates(
    subset=["lastUpdate"]
).sort_values("lastUpdate")
```

```python
drop_name, drop_lastupdate = df_test[
    (df_test.duplicated(subset="lastUpdate", keep=False))
    & df_test["numeric_name"]
].iloc[0][["name", "lastUpdate"]]
```

```python
drop_name, drop_lastupdate
```

```python

```
