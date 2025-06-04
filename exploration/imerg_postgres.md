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

# IMERG Postgres

Checking raster stats database IMERG data is close enough to previous methodology

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.datasources import imerg
```

```python
df_old = imerg.load_imerg_mean(version=7, recent=True)
```

```python
df_new = imerg.load_imerg_from_postgres(recent=True)
```

```python
df_compare = df_old.merge(df_new, on="date", suffixes=("_old", "_new"))
```

```python
df_compare["error"] = df_compare["mean_new"] - df_compare["mean_old"]
```

```python
df_compare["rel_error"] = df_compare["error"] / df_compare["mean_old"]
```

```python
df_compare["error"].hist()
```

```python
df_compare.plot.scatter(x="mean_old", y="error")
```

```python
df_compare.plot.scatter(x="mean_old", y="mean_new")
```

Looks good enough, very unlikely that ≈1mm change in rainfall will affect triggering
