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

# IMERG comparison

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import matplotlib.pyplot as plt

from src.datasources import imerg, chirps
```

```python
df_imerg = imerg.load_imerg_mean()
df_imerg
```

```python
df_chirps = chirps.load_raster_stats()
df_chirps = df_chirps[["T", "mean"]]
df_chirps = df_chirps.rename(columns={"T": "date"})
df_chirps
```

```python
compare = df_chirps.merge(df_imerg, on="date", suffixes=["_ch", "_im"])
```

```python
fig, ax = plt.subplots(dpi=300)
compare.plot(
    x="mean_ch",
    y="mean_im",
    marker=".",
    linewidth=0,
    ax=ax,
    legend=False,
    markersize=1,
)
corr = compare.corr().loc["mean_ch", "mean_im"]
ax.set_ylabel("IMERG v6 Late")
ax.set_xlabel("CHIRPS daily")
ax.set_ylim(bottom=0)
ax.set_xlim(left=0)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title(f"All months, Correlation: {corr:.2f}")
```

```python
im_max = compare["mean_im"].max()
ch_max = compare["mean_ch"].max()

for month, group in compare.groupby(compare["date"].dt.month):
    fig, ax = plt.subplots()
    group.plot(
        x="mean_ch", y="mean_im", ax=ax, marker=".", linewidth=0, legend=False
    )
    corr = group.corr().loc["mean_ch", "mean_im"]
    ax.set_ylim(0, im_max)
    ax.set_xlim(0, ch_max)
    ax.set_ylabel("IMERG v6 Late")
    ax.set_xlabel("CHIRPS daily")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_title(f"Month: {month}, Correlation: {corr:.2f}")
```

```python
corr.loc["mean_ch", "mean_im"]
```
