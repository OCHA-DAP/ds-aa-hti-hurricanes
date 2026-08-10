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

# Exposure-impact comparison

Comparison of exposure estimates with DGPC impact from
https://public.tableau.com/app/profile/protection.civile.haiti/viz/Haitivaluationdesdgtsetanalysedebesoins-OuraganMelissa/0_disclaimer?publish=yes

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import re

import ocha_stratus as stratus
import statsmodels.api as sm
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import EngFormatter
from matplotlib.colors import BoundaryNorm, ListedColormap

from src.utils import plotting
from src.datasources import codab
from src.constants import *
from src.utils.blob import PROJECT_PREFIX
```

```python
blob_name = (
    f"{PROJECT_PREFIX}/processed/hti_melissa_adm2_wind_rain_exposure.parquet"
)
df_exp = stratus.load_parquet_from_blob(blob_name)
```

```python
adm1 = codab.load_codab_from_blob(admin_level=1)
```

```python
df_exp_adm1 = (
    df_exp.groupby(["ADM1_PCODE", "ADM1_FR"])
    .sum(numeric_only=True)
    .reset_index()
)
```

```python
df_exp_adm1
```

```python
df_exp_adm1["dgpc_affected"] = [
    13_665,
    8_880,
    0,
    0,
    15_085,
    0,
    136_825,
    30_475,
    7_930,
    41_685,
]
```

```python
df_exp_adm1.corr(numeric_only=True)["dgpc_affected"]
```

```python
cols = ["exp_34_knots", "exp_400_mm", "dgpc_affected"]
fig, ax = plt.subplots(dpi=200)

df_exp_adm1.sort_values("dgpc_affected", ascending=False).plot.bar(
    x="ADM1_FR",
    y=cols,
    label=["34 knot exposure", "400 mm exposure", "Impact [DGPC]"],
    ax=ax,
)
ax.yaxis.set_major_formatter(EngFormatter(unit=""))
ax.set_xlabel("Département")
ax.set_title(
    "Hurricane Melissa in Haiti:\ncomparison between hazard exposure and impact"
)
[ax.spines[x].set_visible(False) for x in ["top", "right"]]
```

```python
def plot_exposure_vs_affected(
    df,
    x_col,
    x_label,
    y_col="dgpc_affected",
    y_label="Population affected [DGPC]",
    label_col="ADM1_FR",
):
    fig, ax = plt.subplots(dpi=200, figsize=(7, 7))

    # Scatter plot
    df.plot(
        x=x_col,
        y=y_col,
        marker=".",
        linewidth=0,
        ax=ax,
        legend=False,
        color="k",
    )

    # Annotate each point
    for _, row in df.iterrows():
        ax.annotate(
            f"  {row[label_col]}",
            row[[x_col, y_col]],
            ha="left",
            va="center",
            fontsize=7,
        )

    # ---- Trendline through origin ----
    x = df[x_col].values
    y = df[y_col].values

    # Drop NaNs
    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]

    # Linear regression without intercept: y = b * x
    slope = np.sum(x * y) / np.sum(x * x)
    x_fit = np.linspace(0, x.max(), 100)
    y_fit = slope * x_fit
    ax.plot(
        x_fit,
        y_fit,
        color="dodgerblue",
        linestyle="--",
        label=f"Trend: y = {slope:.2f}x",
    )

    # ---- Style ----
    [ax.spines[side].set_visible(False) for side in ["top", "right"]]
    ax.xaxis.set_major_formatter(EngFormatter(unit=""))
    ax.yaxis.set_major_formatter(EngFormatter(unit=""))
    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    # Label slope at end of line
    ax.text(
        x_fit[-1],
        y_fit[-1],
        f"slope = {slope:.2f}",
        fontsize=9,
        color="dodgerblue",
        ha="left",
        va="bottom",
    )

    return ax
```

```python
plot_exposure_vs_affected(
    df_exp_adm1,
    x_col="exp_400_mm",
    x_label="Population exposed to ≥ 400 mm rainfall",
)
```

```python
plot_exposure_vs_affected(
    df_exp_adm1,
    x_col="exp_34_knots",
    x_label="Population exposed to ≥ 34 knots wind speed",
)
```

```python
1 / 0.19
```

```python
fig, ax = plt.subplots(dpi=200, figsize=(7, 7))
df_exp_adm1.plot(
    x="exp_400_mm",
    y="dgpc_affected",
    marker=".",
    linewidth=0,
    ax=ax,
    legend=False,
    color="k",
)

# Annotations
for _, row in df_exp_adm1.iterrows():
    ax.annotate(
        f'  {row["ADM1_FR"]}',
        row[["exp_400_mm", "dgpc_affected"]],
        ha="left",
        va="center",
        fontsize=7,
    )

# ---- Trendline through origin ----
x = df_exp_adm1["exp_400_mm"].values
y = df_exp_adm1["dgpc_affected"].values

# Drop NaNs
mask = np.isfinite(x) & np.isfinite(y)
x = x[mask]
y = y[mask]

# Linear regression without intercept: y = b * x
slope = np.sum(x * y) / np.sum(x * x)
x_fit = np.linspace(0, x.max(), 100)
y_fit = slope * x_fit
ax.plot(
    x_fit,
    y_fit,
    color="dodgerblue",
    linestyle="--",
    label=f"Trend: y = {slope:.2f}x",
)

# ---- Style ----
[ax.spines[x].set_visible(False) for x in ["top", "right"]]
ax.xaxis.set_major_formatter(EngFormatter(unit=""))
ax.yaxis.set_major_formatter(EngFormatter(unit=""))
ax.set_ylim(bottom=0)
ax.set_xlim(left=0)

ax.set_xlabel("Population exposed to ≥ 400 mm rainfall")
ax.set_ylabel("Population affected [DGPC]")

ax.text(
    x_fit[-1],
    y_fit[-1],
    f"slope = {slope:.2f}",
    fontsize=9,
    color="dodgerblue",
    ha="left",
    va="bottom",
)
```

```python
for x in ["exp_34_knots", "exp_400_mm"]:
    df_exp_adm1[f"{x}_factor"] = df_exp_adm1[x] / df_exp_adm1["dgpc_affected"]
```

```python
df_exp_adm1.mean(numeric_only=True)
```

```python
# Define predictors (independent variables)
X = df_exp_adm1[["exp_34_knots", "exp_400_mm"]]

# Add a constant (intercept) to the predictors
X = sm.add_constant(X)

# Define response variable (dependent variable)
y = df_exp_adm1["dgpc_affected"]

# Fit the OLS regression model
model = sm.OLS(y, X).fit()

# Print the summary of the regression
print(model.summary())
```
