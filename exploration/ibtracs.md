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

# Tracks

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

from src.datasources import ibtracs, chirps, impact, imerg
from src.utils import blob
```

```python
# ibtracs.process_hti_distances()
```

```python
tracks = ibtracs.load_hti_distances()
```

```python
affected = impact.load_hti_impact()
affected = (
    affected.groupby("sid")["affected_population"]
    .first()
    .astype(int)
    .reset_index()
)
```

```python
hurricanes = tracks.groupby("sid")[["name", "time"]].first().reset_index()
hurricanes["year"] = hurricanes["time"].dt.year
hurricanes = hurricanes.drop(columns=["time"])
```

```python
rain = chirps.load_raster_stats()
rain["roll3_sum"] = (
    rain["mean"]
    .rolling(window=3, center=True, min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)
rain["roll2_sum_bw"] = (
    rain["mean"]
    .rolling(window=2, center=True, min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)
rain["roll2_sum_fw"] = rain["roll2_sum_bw"].shift(-1).fillna(0)
```

```python
rain_imerg = imerg.load_imerg_mean()
rain_imerg["roll2"] = (
    rain_imerg["mean"]
    .rolling(window=2, center=True, min_periods=1)
    .sum()
    .shift(-1)
    .fillna(0)
)
```

```python
# d_thresh = 230
# d_thresh = 380
d_thresh = 20

tracks_f = tracks[tracks["distance (m)"] < d_thresh * 1000]

dicts = []
for sid, group in tracks_f.groupby("sid"):
    start_day = pd.Timestamp(group["time"].min().date() - pd.Timedelta(days=1))
    end_day = pd.Timestamp(group["time"].max().date() + pd.Timedelta(days=1))
    rain_f = rain[(rain["T"] >= start_day) & (rain["T"] <= end_day)]

    end_day_early = pd.Timestamp(group["time"].max().date())
    rain_f_early = rain[
        (rain["T"] >= start_day) & (rain["T"] <= end_day_early)
    ]
    rain_imerg_f = rain_imerg[
        (rain_imerg["date"] >= start_day)
        & (rain_imerg["date"] <= end_day_early)
    ]
    dicts.append(
        {
            "sid": sid,
            "max_wind": group["usa_wind"].max(),
            "mean_wind": group["usa_wind"].mean(),
            "max_mean_rain": rain_f["mean"].max(),
            "mean_mean_rain": rain_f["mean"].mean(),
            "sum_mean_rain": rain_f["mean"].sum(),
            "max_q80_rain": rain_f["q80"].max(),
            "mean_q80_rain": rain_f["q80"].max(),
            "sum_q80_rain": rain_f["q80"].sum(),
            "max_q90_rain": rain_f["q90"].max(),
            "mean_q90_rain": rain_f["q90"].max(),
            "sum_q90_rain": rain_f["q90"].sum(),
            "max_roll3_sum_rain": rain_f["roll3_sum"].max(),
            "max_roll2_sum_rain": rain_f_early["roll2_sum_fw"].max(),
            "max_roll2_sum_rain_imerg": rain_imerg_f["roll2"].max(),
            # "max_roll2_bw_sum_rain": rain_f["roll2_sum_bw"].max(),
        }
    )

stats = pd.DataFrame(dicts)
stats = stats.merge(hurricanes, on="sid")
stats = stats.merge(affected, on="sid", how="left")
stats["rank"] = stats["affected_population"].rank()
stats
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/stats_{d_thresh}km.csv"
blob.upload_csv_to_blob(blob_name, stats)
```

```python
def sid_color(sid):
    color = "blue"
    if sid in ibtracs.CERF_SIDS:
        color = "red"
    # elif sid in ibtracs.IMPACT_SIDS:
    #     color = "orange"
    return color


stats["marker_size"] = stats["affected_population"] / 6e2
stats["marker_size"] = stats["marker_size"].fillna(1)
stats["color"] = stats["sid"].apply(sid_color)

fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

ax.scatter(
    stats["max_wind"],
    stats["max_q90_rain"],
    s=stats["marker_size"],
    c=stats["color"],
    alpha=0.5,
    edgecolors="none",
)

for j, txt in enumerate(
    stats["name"].str.capitalize() + "\n" + stats["year"].astype(str)
):
    ax.annotate(
        txt.capitalize(),
        (stats["max_wind"][j] + 0.5, stats["max_q90_rain"][j]),
        ha="left",
        va="center",
        fontsize=7,
    )

ax.axvline(x=50, color="lightgray", linestyle="dashed")
ax.axhline(y=50, color="lightgray", linestyle="dashed")
ax.fill_between(
    np.arange(50, 200, 1), 50, 200, color="gold", alpha=0.2, zorder=-1
)

ax.set_xlim(right=155, left=0)
ax.set_ylim(top=120, bottom=0)

ax.set_xlabel("Vitesse de vent maximum (noeuds)")
ax.set_ylabel(
    "Précipitations journalières sur 10% de la superficie maximum (mm)"
)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title(
    f"Comparaison de précipitations, vent, et impact\n"
    f"Seuil de distance = {d_thresh} km"
)
```

```python
def sid_color(sid):
    color = "blue"
    if sid in ibtracs.CERF_SIDS:
        color = "red"
    # elif sid in ibtracs.IMPACT_SIDS:
    #     color = "orange"
    return color


stats["marker_size"] = stats["affected_population"] / 6e2
stats["marker_size"] = stats["marker_size"].fillna(1)
stats["color"] = stats["sid"].apply(sid_color)

fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

ax.scatter(
    stats["max_wind"],
    stats["max_mean_rain"],
    s=stats["marker_size"],
    c=stats["color"],
    alpha=0.5,
    edgecolors="none",
)

for j, txt in enumerate(
    stats["name"].str.capitalize() + "\n" + stats["year"].astype(str)
):
    ax.annotate(
        txt.capitalize(),
        (stats["max_wind"][j] + 0.5, stats["max_mean_rain"][j]),
        ha="left",
        va="center",
        fontsize=7,
    )

ax.axvline(x=50, color="lightgray", linestyle="dashed")
ax.axhline(y=30, color="lightgray", linestyle="dashed")
ax.fill_between(
    np.arange(50, 200, 1), 30, 200, color="gold", alpha=0.2, zorder=-1
)

ax.set_xlim(right=155, left=0)
ax.set_ylim(top=70, bottom=0)

ax.set_xlabel("Vitesse de vent maximum (noeuds)")
ax.set_ylabel(
    "Précipitations journalières maximum, moyenne sur toute la superficie (mm)"
)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title(
    f"Comparaison de précipitations, vent, et impact\n"
    f"Seuil de distance = {d_thresh} km"
)
```

```python
rain_col = "max_roll2_sum_rain"


def sid_color(sid):
    color = "blue"
    if sid in ibtracs.CERF_SIDS:
        color = "red"
    # elif sid in ibtracs.IMPACT_SIDS:
    #     color = "orange"
    return color


stats["marker_size"] = stats["affected_population"] / 6e2
stats["marker_size"] = stats["marker_size"].fillna(1)
stats["color"] = stats["sid"].apply(sid_color)

fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

ax.scatter(
    stats["max_wind"],
    stats[rain_col],
    s=stats["marker_size"],
    c=stats["color"],
    alpha=0.5,
    edgecolors="none",
)

for j, txt in enumerate(
    stats["name"].str.capitalize() + "\n" + stats["year"].astype(str)
):
    ax.annotate(
        txt.capitalize(),
        (stats["max_wind"][j] + 0.5, stats[rain_col][j]),
        ha="left",
        va="center",
        fontsize=7,
    )

rain_thresh = 40
ax.axvline(x=50, color="lightgray", linestyle="dashed")
ax.axhline(y=rain_thresh, color="lightgray", linestyle="dashed")
ax.fill_between(
    np.arange(50, 200, 1), rain_thresh, 200, color="gold", alpha=0.2, zorder=-1
)

ax.set_xlim(right=155, left=0)
ax.set_ylim(top=100, bottom=0)

ax.set_xlabel("Vitesse de vent maximum (noeuds)")
ax.set_ylabel(
    "Précipitations pendant deux jours consécutifs maximum, "
    "moyenne sur toute la superficie (mm)"
)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title(
    f"Comparaison de précipitations, vent, et impact\n"
    f"Seuil de distance = {d_thresh} km"
)
```

```python
rain_col = "max_roll2_sum_rain_imerg"


def sid_color(sid):
    color = "blue"
    if sid in ibtracs.CERF_SIDS:
        color = "red"
    # elif sid in ibtracs.IMPACT_SIDS:
    #     color = "orange"
    return color


stats["marker_size"] = stats["affected_population"] / 6e2
stats["marker_size"] = stats["marker_size"].fillna(1)
stats["color"] = stats["sid"].apply(sid_color)

fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

ax.scatter(
    stats["max_wind"],
    stats[rain_col],
    s=stats["marker_size"],
    c=stats["color"],
    alpha=0.5,
    edgecolors="none",
)

for j, txt in enumerate(
    stats["name"].str.capitalize() + "\n" + stats["year"].astype(str)
):
    ax.annotate(
        txt.capitalize(),
        (stats["max_wind"][j] + 0.5, stats[rain_col][j]),
        ha="left",
        va="center",
        fontsize=7,
    )

rain_thresh = 40
s_thresh = 70
ax.axvline(x=s_thresh, color="lightgray", linestyle="dashed")
ax.axhline(y=rain_thresh, color="lightgray", linestyle="dashed")
ax.fill_between(
    np.arange(s_thresh, 200, 1), rain_thresh, 200, color="gold", alpha=0.2, zorder=-1
)

ax.set_xlim(right=155, left=0)
ax.set_ylim(bottom=0, top=170)

ax.set_xlabel("Vitesse de vent maximum (noeuds)")
ax.set_ylabel(
    "Précipitations pendant deux jours consécutifs maximum, "
    "moyenne sur toute la superficie (mm)"
)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title(
    f"Comparaison de précipitations, vent, et impact\n"
    f"Seuil de distance = {d_thresh} km"
)
```

```python
rain_col = "max_roll2_sum_rain_imerg"


def sid_color(sid):
    color = "blue"
    if sid in ibtracs.CERF_SIDS:
        color = "red"
    # elif sid in ibtracs.IMPACT_SIDS:
    #     color = "orange"
    return color


stats["marker_size"] = stats["affected_population"] / 6e2
stats["marker_size"] = stats["marker_size"].fillna(1)
stats["color"] = stats["sid"].apply(sid_color)

fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

ax.scatter(
    stats["max_wind"],
    stats[rain_col],
    s=stats["marker_size"],
    c=stats["color"],
    alpha=0.5,
    edgecolors="none",
)

for j, txt in enumerate(
    stats["name"].str.capitalize() + "\n" + stats["year"].astype(str)
):
    ax.annotate(
        txt.capitalize(),
        (stats["max_wind"][j] + 0.5, stats[rain_col][j]),
        ha="left",
        va="center",
        fontsize=7,
    )

rain_thresh = 60
ax.axvline(x=50, color="lightgray", linestyle="dashed")
ax.axhline(y=rain_thresh, color="lightgray", linestyle="dashed")
ax.fill_between(
    np.arange(50, 200, 1), rain_thresh, 200, color="gold", alpha=0.2, zorder=-1
)

ax.set_xlim(right=155, left=0)
ax.set_ylim(bottom=0, top=170)

ax.set_xlabel("Vitesse de vent maximum (noeuds)")
ax.set_ylabel(
    "Précipitations pendant deux jours consécutifs maximum, "
    "moyenne sur toute la superficie (mm)"
)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title(
    f"Comparaison de précipitations, vent, et impact\n"
    f"Seuil de distance = {d_thresh} km"
)
```

```python
dicts = []
for d_thresh in range(0, 501, 10):
    tracks_f = tracks[tracks["distance (m)"] <= d_thresh * 1000]
    for sid, group in tracks_f.groupby("sid"):
        dicts.append(
            {
                "sid": sid,
                "max_wind": group["usa_wind"].max(),
                "d": d_thresh,
            }
        )

wind_stats = pd.DataFrame(dicts)
wind_stats = wind_stats.merge(hurricanes, on="sid")
wind_stats = wind_stats.merge(affected, on="sid", how="left")
wind_stats["rank"] = wind_stats["affected_population"].rank()
wind_stats["nameyear"] = (
    wind_stats["name"].str.capitalize() + " " + wind_stats["year"].astype(str)
)
wind_stats
```

```python
wind_stats.dtypes
```

```python
x_cutoff = 64
y_cutoff = 20


def sid_color(sid):
    color = "blue"
    if sid in ibtracs.CERF_SIDS:
        color = "red"
    # elif sid in ibtracs.IMPACT_SIDS:
    #     color = "orange"
    return color


wind_stats["marker_size"] = wind_stats["affected_population"] / 5e4 + 0.3
wind_stats["marker_size"] = wind_stats["marker_size"].fillna(0.3)
wind_stats["color"] = wind_stats["sid"].apply(sid_color)

fig, ax = plt.subplots(figsize=(8, 8), dpi=300)

for nameyear, group in wind_stats.groupby("nameyear"):
    ax.plot(
        group["max_wind"],
        group["d"],
        linewidth=group["marker_size"].iloc[0],
        c=group["color"].iloc[0],
        alpha=0.5,
        # edgecolors="none",
    )
    ax.annotate(
        nameyear + "  ",
        (group["max_wind"].min(), group["d"].min()),
        ha="center",
        va="bottom",
        fontsize=7,
        rotation=270,
    )

ax.axvline(x=x_cutoff, color="lightgray", linestyle="dashed")
ax.axhline(y=y_cutoff, color="lightgray", linestyle="dashed")
ax.fill_between(
    np.arange(x_cutoff, 200, 1),
    0,
    y_cutoff,
    color="gold",
    alpha=0.2,
    zorder=-1,
)

ax.set_xlim(right=155, left=0)
ax.set_ylim(top=100, bottom=0)

ax.set_xlabel("Vitesse de vent maximum (noeuds)")
ax.set_ylabel("Distance à Haïti (km)")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title("Comparaison de distance, vent, et impact")
```

```python
d_thresh = 250
tracks_f = tracks[tracks["distance (m)"] < d_thresh * 1000]

max_rain = rain[["mean"]].max().max()

for year in range(2000, 2024):
    fig, ax = plt.subplots(figsize=(15, 5), dpi=300)
    rain[rain["T"].dt.year == year].set_index("T")["mean"].plot(
        ax=ax, linewidth=0.5, color="k", linestyle="-"
    )
    # rain[rain["T"].dt.year == year].set_index("T")["q80"].plot(
    #     ax=ax, linewidth=0.5, color="k", linestyle="--", legend=True
    # )

    for sid, group in tracks_f[tracks_f["time"].dt.year == year].groupby(
        "sid"
    ):
        if sid in ibtracs.CERF_SIDS:
            color = "red"
        elif sid in ibtracs.IMPACT_SIDS:
            color = "orange"
        else:
            color = "grey"
        start_day = group["time"].min() - pd.Timedelta(days=1)
        end_day = group["time"].max() + pd.Timedelta(days=1)
        ax.axvspan(
            start_day,
            end_day,
            facecolor=color,
            alpha=0.1,
            edgecolor="none",
        )
        ax.text(
            start_day,
            max_rain,
            group.iloc[0]["name"].capitalize(),
            rotation=90,
            va="top",
            ha="right",
            color=color,
        )

    ax.set_ylim(bottom=0, top=max_rain)
    ax.set_ylabel("Précipitations quotidiennes\nmoyennes en Haïti(mm)")
    ax.set_xlabel("Date")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
```

```python

```

```python
years = range(2000, 2024)

stats["rank"] = stats["affected_population"].rank(ascending=False)
stats["target"] = stats["rank"] < 11

P = len(stats[stats["target"]])
N = len(stats[~stats["target"]])

rp_target_s = len(years) / stats[stats["target"]]["sid"].nunique()
rp_target_y = len(years) / stats[stats["target"]]["year"].nunique()
print(f"{rp_target_s=}")
print(f"{rp_target_y=}")

s_threshs = range(30, int(stats["max_wind"].max()) + 11, 10)
p_threshs = range(10, int(stats["max_mean_rain"].max()) + 11, 10)

# s_threshs = range(10, 151, 10)
# p_threshs = range(10, 101, 10)

dicts = []
for s_thresh in s_threshs:
    for p_thresh in p_threshs:
        dff = stats[
            (stats["max_wind"] >= s_thresh)
            & (stats["max_mean_rain"] >= p_thresh)
        ]
        dicts.append(
            {
                "s_thresh": s_thresh,
                "p_thresh": p_thresh,
                "n_storms": len(dff),
                "n_years": dff["year"].nunique(),
                "TP": len(dff[dff["target"]]),
                "FP": len(dff[~dff["target"]]),
            }
        )

rp = pd.DataFrame(dicts)
rp["FN"] = P - rp["TP"]
rp["TN"] = N - rp["FP"]
rp["rp_y"] = len(years) / rp["n_years"]
rp["rp_s"] = len(years) / rp["n_storms"]
```

```python
dff
```

```python
stats.sort_values("affected_population", ascending=False)
```

```python
len(years)
```

```python
stats
```

```python
corr_cols = ["max_wind", "max_mean_rain", "affected_population", "rank"]
corr = stats[corr_cols].corr()
corr
```

```python
def add_confusion_matrix_metrics(df):
    # Define a helper function for MCC to handle division by zero
    def calculate_mcc(row):
        TP, TN, FP, FN = row["TP"], row["TN"], row["FP"], row["FN"]
        mcc_denominator = np.sqrt(
            (TP + FP) * (TP + FN) * (TN + FP) * (TN + FN)
        )
        if mcc_denominator == 0:
            return (
                np.nan
            )  # Return NaN if the denominator is 0 to avoid division by zero
        else:
            return ((TP * TN) - (FP * FN)) / mcc_denominator

    # Vectorized calculations for each metric
    df["Accuracy"] = (df["TP"] + df["TN"]) / (
        df["TP"] + df["TN"] + df["FP"] + df["FN"]
    )
    df["Precision"] = df["TP"] / (df["TP"] + df["FP"])
    df["Recall"] = df["TP"] / (df["TP"] + df["FN"])
    df["F1 Score"] = (
        2 * (df["Precision"] * df["Recall"]) / (df["Precision"] + df["Recall"])
    )
    df["Specificity"] = df["TN"] / (df["TN"] + df["FP"])
    df["False Positive Rate"] = df["FP"] / (df["FP"] + df["TN"])
    df["Negative Predictive Value"] = df["TN"] / (df["TN"] + df["FN"])
    df["MCC"] = df.apply(calculate_mcc, axis=1)

    # Handling potential division by zero or undefined calculations
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return df


rp = add_confusion_matrix_metrics(rp)
```

```python
rp_var = "rp_y"
rp_min, rp_max = 2.5, np.inf
rp_f = rp[(rp[rp_var] >= rp_min) & (rp[rp_var] <= rp_max)]

for plot_val in ["rp_s", "rp_y", "MCC", "F1 Score"]:
    pivot_table = rp_f.pivot(
        index="p_thresh", columns="s_thresh", values=plot_val
    )
    fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
    sns.heatmap(pivot_table, cmap="viridis", ax=ax, annot=True)
    ax.set_title(plot_val)
    ax.set_xlabel("Seuil de vitesse de vent (noeuds)")
    ax.set_ylabel("Seuil de précipitations journalières (mm)")
    ax.invert_yaxis()
```

```python
pivot_table = rp.pivot(index="p_thresh", columns="s_thresh", values="rp_y")

fig, ax = plt.subplots(figsize=(10, 5), dpi=300)

sns.heatmap(pivot_table, cmap="viridis", ax=ax, annot=True)
ax.set_title("Heatmap of rp by s_thresh and p_thresh")
ax.set_xlabel("s_thresh")
ax.set_ylabel("p_thresh")
ax.invert_yaxis()
```

```python

```
