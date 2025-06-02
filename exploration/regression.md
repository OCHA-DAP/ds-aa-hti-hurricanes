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

# Regression

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import statsmodels.api as sm

from src.utils import blob
```

```python
d_thresh = 230
blob_name = f"{blob.PROJECT_PREFIX}/processed/stats_{d_thresh}km.csv"
df = blob.load_csv_from_blob(blob_name)
df["affected_population"] = df["affected_population"].fillna(0)
df["rank"] = df["affected_population"].rank(ascending=False)
df["target"] = df["rank"] <= 10
df = df.sort_values("affected_population", ascending=False)
df
```

```python
for col in ["target", "affected_population"]:
    X = df[["max_wind", "max_roll2_sum_rain"]]
    y = df[col]
    X = sm.add_constant(X)

    model = sm.OLS(y, X).fit()

    print(model.summary())
    df[f"pred_{col}"] = model.predict(X)
    df[f"pred_{col}_rank"] = df[f"pred_{col}"].rank(ascending=False)
    df[f"pred_{col}_bool"] = df[f"pred_{col}_rank"] <= 10
```

```python
target_years = 8
for thresh in df["predicted_affected_population"].sort_values(ascending=False):
    n_years = df[df["predicted_affected_population"] >= thresh][
        "year"
    ].nunique()
    if n_years > target_years:
        break
    prev_thresh = thresh

df["pred_target"] = df["predicted_affected_population"] >= prev_thresh
```

```python
df
```

```python
df.to_csv("temp/hti_trigger_regression.csv", index=False)
```
