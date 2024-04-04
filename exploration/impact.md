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

# Impact

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt

from src.datasources import impact, codab
```

```python
adm = codab.load_codab()
adm2 = adm.dissolve("ADM2_PCODE")
```

```python
adm2.plot()
```

```python
df = impact.load_hti_impact()
```

```python
df
```

```python
df["typhoon_name"].unique()
```

```python
for sid, group in df.groupby("sid"):
    fig, ax = plt.subplots()
    gdf_plot = adm2.merge(
        group[["affected_population", "ADM2_PCODE"]], on="ADM2_PCODE"
    )
    adm2.plot(ax=ax, color="grey")
    gdf_plot.plot(ax=ax, color="orange")
    name, affected, year = group.iloc[0][
        ["typhoon_name", "affected_population", "Year"]
    ]
    ax.set_title(
        f"{name.strip().capitalize()} {year}\nPopulation totale affectée: {affected:,.0f}"
    )
    ax.axis("off")
```
