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

# Trigger comparison

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
from tqdm.notebook import tqdm
from matplotlib.ticker import FuncFormatter
from matplotlib.colors import ListedColormap

from src.datasources import ibtracs, chirps, impact, gtcm, imerg
```

Adjust `TARGET_ACT` to determine how many storms we want to target.
Only needs to be done if you want to calculate accuracy metrics
(TP, FP, etc)

```python
# adjust TARGET_ACT to determine how many storms we want to target

# 3yr RP
# TARGET_ACT = 8
# 4yr RP
# TARGET_ACT = 6
# 5yr RP
TARGET_ACT = 5
MAX_DISTANCE = 500
CAT_1 = 64
CAT_2 = 83
CAT_3 = 96
CAT_4 = 113
CAT_5 = 137
CATS = [CAT_1, CAT_2, CAT_3, CAT_4, CAT_5]
JEANNE = "2004258N16300"
IVAN = "2004247N10332"
```

This loads the impact predictions from the impact model.
We don't currently use the impact model so you can skip
this cell.

```python
# load impact predictions from impact model
# currently, not being used, so this cell can be skipped
impactmodel = gtcm.load_gtcm_impact()
impactmodel["nameyear"] = (
    impactmodel["event"].str.capitalize()
    + " "
    + impactmodel["year"].astype(str)
)
impactmodel = impactmodel.sort_values("prediction_adm0", ascending=False)
for x in impactmodel["prediction_adm0"]:
    dff = impactmodel[impactmodel["prediction_adm0"] >= x]
    if dff["year"].nunique() == TARGET_ACT:
        break

impactmodel["model_trigger"] = impactmodel["prediction_adm0"] >= x
impactmodel
```

Load observational tracks with distance to Haiti already calculated.

```python
tracks = ibtracs.load_hti_distances()
```

```python
MAX_SPEED = tracks["usa_wind"].max()
```

```python
MAX_SPEED
```

Load impact data.

The `affected_population_adj` was just adjusting the impact for storms
whose impact may have been attributed to a different storm.
We decided to stick with the normal impact, so this can be ignored.

```python
affected = impact.load_hti_impact()
affected = (
    affected.groupby("sid")["affected_population"]
    .first()
    .astype(int)
    .reset_index()
)
affected["rank"] = affected["affected_population"].rank(ascending=False)


def adjust_impact(row):
    if row["sid"] == IVAN:
        return affected[affected["sid"].isin([IVAN, JEANNE])][
            "affected_population"
        ].sum()
    elif row["sid"] == JEANNE:
        return 0
    else:
        return row["affected_population"]


affected["affected_population_adj"] = affected.apply(adjust_impact, axis=1)
affected["rank_adj"] = affected["affected_population_adj"].rank(
    ascending=False
)
```

Join impact to storms. Again you can ignore the stuff about
adjusted impact.

```python
hurricanes = (
    tracks[tracks["distance (m)"] <= MAX_DISTANCE * 1000]
    .groupby("sid")[["name", "time"]]
    .first()
    .reset_index()
)
hurricanes["year"] = hurricanes["time"].dt.year
hurricanes = hurricanes.drop(columns=["time"])
hurricanes = hurricanes.merge(affected, on="sid", how="left")
hurricanes = hurricanes.sort_values("rank")
display(hurricanes[:20])
TARGET_YEARS = hurricanes["year"].unique()[:TARGET_ACT]
print(TARGET_YEARS)

# select desired return period
# 3 year RP
# MAX_RANK = 10

# 4 year RP
# MAX_RANK = 8

# 5 year RP
MAX_RANK = 7

hurricanes["target"] = hurricanes["rank"] <= MAX_RANK
TARGET_SIDS = hurricanes[hurricanes["target"]]["sid"]

hurricanes = hurricanes.sort_values("rank_adj")
display(hurricanes[:20])
TARGET_YEARS_ADJ = hurricanes["year"].unique()[:TARGET_ACT]
print(TARGET_YEARS_ADJ)
MAX_RANK_ADJ = 10
hurricanes["target"] = hurricanes["rank"] <= MAX_RANK_ADJ
TARGET_SIDS_ADJ = hurricanes[hurricanes["target"]]["sid"]
```

```python
hurricanes.sort_values("year")["year"].unique()
```

```python
TARGET_SIDS
```

Load the rain raster stats,
and calculate the rolling values.
`fw` and `bw` is just to account for the 2-day rolling
not having a center date.

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
rain
```

Load IMERG rain data.
(The previous analysis was done with CHIRPS rainfall, before
we started using IMERG. It is also better matched to the
CHIRPS-GEFS forecast, so we will use these figures for showing how
bad a forecast is.)

```python
rain_imerg = imerg.load_imerg_mean()
rain_imerg["roll2"] = (
    rain_imerg["mean"].rolling(window=2, center=True, min_periods=1).sum()
)
```

```python
MAX_RAIN = max(rain.drop(columns="T").max().max(), rain_imerg["mean"].max())
```

```python
MAX_RAIN
```

```python
50 * 18 * 4 * 10 * 17
```

First cycle over distance thresholds to get stats for each
distance cut-off. If you already have a distance threshold in mind
you can just calculate each storms stats for that one threshold.

The commented-out section at the bottom records the results for
different ways to aggregate the rainfall. I stopped using this
to save time after noting that `mean` seemed to work the best.

```python
d_threshs = range(0, MAX_DISTANCE + 1, 10)
s_threshs = [x for x in range(0, int(MAX_SPEED) + 1, 10)] + CATS
p_threshs = range(0, int(MAX_RAIN) + 1, 10)
```

```python
dicts = []
for d_thresh in d_threshs:
    tracks_f = tracks[tracks["distance (m)"] <= d_thresh * 1000]

    for sid, group in tracks_f.groupby("sid"):
        start_day = pd.Timestamp(
            group["time"].min().date() - pd.Timedelta(days=1)
        )
        end_day = pd.Timestamp(
            group["time"].max().date() + pd.Timedelta(days=1)
        )

        rain_f = rain[(rain["T"] >= start_day) & (rain["T"] <= end_day)]
        end_day_early = pd.Timestamp(group["time"].max().date())
        rain_f_early = rain[
            (rain["T"] >= start_day) & (rain["T"] <= end_day_early)
        ]
        start_day_late = pd.Timestamp(group["time"].min().date())
        rain_f_short = rain[
            (rain["T"] >= start_day_late) & (rain["T"] <= end_day_early)
        ]
        rain_imerg_f = rain_imerg[
            (rain_imerg["date"] >= start_day_late)
            & (rain_imerg["date"] <= end_day)
        ]

        dict_out = {
            "sid": sid,
            "max_wind": group["usa_wind"].max(),
            # "mean_wind": group["usa_wind"].mean(),
            "max_mean_rain": rain_f["mean"].max(),
            # "mean_mean_rain": rain_f["mean"].mean(),
            "sum_mean_rain": rain_f["mean"].sum(),
            "max_roll3_sum_rain": rain_f_short["roll3_sum"].max(),
            "max_roll2_sum_rain": rain_f_early["roll2_sum_fw"].max(),
            # "max_roll2_bw_sum_rain": rain_f["roll2_sum_bw"].max(),
            "max_roll2_sum_rain_imerg": rain_imerg_f["roll2"].max(),
            "d_thresh": d_thresh,
        }

        # for x in range(10, 91, 10):
        #     dict_out.update(
        #         {
        #             f"max_q{x}_rain": rain_f[f"q{x}"].max(),
        #             f"mean_q{x}_rain": rain_f[f"q{x}"].max(),
        #             f"sum_q{x}_rain": rain_f[f"q{x}"].sum(),
        #         }
        #     )
        dicts.append(dict_out)

stats = pd.DataFrame(dicts)
stats = stats.merge(hurricanes, on="sid")
stats
```

Cycle over rainfall thresholds `p_threshs` and `p_metrics`
(the different ways to aggregate the precipitation).
This isn't shown here, but `mean`
seemed to be the best for Haiti, so that is the only aggregation
that is used for IMERG.

Only save the triggers that have the correct return period
(i.e. `dfff["year"].nunique() == TARGET_ACT`)

```python
# p_metrics = [
#     x for x in stats.columns if "rain" in x and "max" in x and "q" not in x
# ]
p_metrics = [x for x in stats.columns if "rain" in x]
print(p_metrics)

dicts = []


def get_hits(dff, trig_str):
    trig_sids = dff["sid"]
    return {
        "trig_str": trig_str,
        "n_hits": sum(x in TARGET_SIDS.values for x in trig_sids),
        "n_hits_adj": sum(x in TARGET_SIDS_ADJ.values for x in trig_sids),
        "n_year_hits": sum(x in TARGET_YEARS for x in dff["year"].unique()),
        "affected_captured": dff["affected_population"].sum(),
        "affected_captured_adj": dff["affected_population_adj"].sum(),
    }


col_tuples = []
for d_thresh, group in tqdm(stats.groupby("d_thresh")):
    for s_thresh in s_threshs:
        dff = group[group["max_wind"] >= s_thresh]
        if dff["year"].nunique() == TARGET_ACT:
            trig_str = f"d{d_thresh}_s{s_thresh}_NORAIN"
            trig_sids = dff["sid"]
            col_tuples.append((trig_str, hurricanes["sid"].isin(trig_sids)))
            dicts.append(get_hits(dff, trig_str))

        for p_thresh in p_threshs:
            for p_metric in p_metrics:
                # AND condition
                dfff = dff[
                    (dff[p_metric] >= p_thresh) & (dff["max_wind"] >= s_thresh)
                ]
                if dfff["year"].nunique() == TARGET_ACT:
                    trig_str = (
                        f"d{d_thresh}_s{s_thresh}_AND_{p_metric}{p_thresh}"
                    )
                    trig_sids = dfff["sid"]
                    col_tuples.append(
                        (trig_str, hurricanes["sid"].isin(trig_sids))
                    )
                    dicts.append(get_hits(dfff, trig_str))

                # OR condition
                dfff = dff[
                    (dff[p_metric] >= p_thresh) | (dff["max_wind"] >= s_thresh)
                ]
                if dfff["year"].nunique() == TARGET_ACT:
                    trig_str = (
                        f"d{d_thresh}_s{s_thresh}_OR_{p_metric}{p_thresh}"
                    )
                    trig_sids = dfff["sid"]
                    col_tuples.append(
                        (trig_str, hurricanes["sid"].isin(trig_sids))
                    )
                    dicts.append(get_hits(dfff, trig_str))

hits = pd.DataFrame(dicts)

cols = [
    pd.Series(data=col_data, name=col_name)
    for col_name, col_data in col_tuples
]
triggers = pd.concat([hurricanes] + cols, axis=1)
```

```python
triggers["nameyear"] = (
    triggers["name"].str.capitalize() + " " + triggers["year"].astype(str)
)
hits = hits.sort_values("affected_captured", ascending=False)
```

See which triggers performend the best (i.e., would have triggered
for cyclones that affected the most people, in col `affected_captured`)

```python
hits.iloc[:50]
```

Check the triggers that fit with the previously established
230 km distance threshold.

```python
hits[hits["trig_str"].str.contains("d230")].iloc[:50]
```

Save some specific triggers.

```python
trigger_str = "d380_s70_AND_max_roll2_sum_rain_imerg40"
# trigger_str = "d230_s50_AND_max_roll2_sum_rain_imerg60"
cols = [
    "sid",
    "name",
    "year",
    "affected_population",
    "rank",
    "target",
]
triggers[cols + [trigger_str]].iloc[:50].sort_values(
    "affected_population", ascending=False
)
```

```python
trigger_str = "d230_s50_AND_max_roll2_sum_rain40"
cols = [
    "sid",
    "name",
    "year",
    "affected_population",
    "rank",
    "target",
]

filename = f"{trigger_str}_triggers.csv"
triggers[cols + [trigger_str]].sort_values(
    "affected_population", ascending=False
).to_csv(ibtracs.IBTRACS_HTI_PROC_DIR / filename, index=False)
```

```python
triggers
```

```python
triggers[cols + [trigger_str]].sort_values(
    "affected_population", ascending=False
)
```

Different ways at looking at accuracy - can be ignored
for now.

```python
base_cols = [
    "sid",
    "name",
    "year",
    "affected_population",
    "rank",
    "target",
    "affected_population_adj",
    "rank_adj",
]

best_cols = list(hits[hits["n_hits"] == hits["n_hits"].max()]["trig_str"])
best_cols_adj = list(
    hits[hits["n_hits_adj"] == hits["n_hits_adj"].max()]["trig_str"]
)
best_year_cols = list(
    hits[hits["n_year_hits"] == hits["n_year_hits"].max()]["trig_str"]
)

second_best_cols = list(
    hits[hits["n_hits"] == hits["n_hits"].max() - 1]["trig_str"]
)

norain_cols = [x for x in triggers.columns if "NORAIN" in x]

hits_norain = hits[hits["trig_str"].isin(norain_cols)]
best_norain_cols = list(
    hits_norain[hits_norain["n_hits"] == hits_norain["n_hits"].max()][
        "trig_str"
    ]
)
best_norain_years_cols = list(
    hits_norain[
        hits_norain["n_year_hits"] == hits_norain["n_year_hits"].max()
    ]["n_year_hits"]
)

all_bestcols = best_cols + best_norain_cols
all_year_bestcols = best_year_cols + best_norain_years_cols
```

```python
max_plot_rank = triggers[triggers[all_bestcols].any(axis=1)]["rank"].max()
plot_sids = triggers[triggers[all_bestcols].any(axis=1)]["sid"]
```

```python
triggers
```

Below is just plotting.
First the bar chart for impact, then separately the
red boxes to indicate which cyclones would have triggered.
These were then just combined on a slide.

```python
df_plot = triggers[
    (triggers["rank"] <= max_plot_rank) | triggers["sid"].isin(plot_sids)
].copy()
df_plot = df_plot.merge(
    impactmodel[["nameyear", "model_trigger", "prediction_adm0"]],
    on="nameyear",
    how="left",
)
df_plot = df_plot.sort_values("affected_population", ascending=False)

fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
df_plot.plot.bar(x="nameyear", y="affected_population", ax=ax, legend=False)
ax.invert_xaxis()
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_xlabel("")
ax.set_ylabel("Nombre de personnes affectées")
ax.set_ylim(top=500000)
formatter = FuncFormatter(lambda x, pos: f"{int(x):,}".replace(",", " "))
ax.yaxis.set_major_formatter(formatter)
```

```python
# 3 year RP
plot_cols = [
    "d230_s50_AND_max_roll2_sum_rain40"
    # "d230_s70_AND_max_roll3_sum_rain50"
    # "d230_s50_AND_max_q90_rain50",
    # "d230_s50_AND_max_mean_rain30",
    # "d240_s40_AND_max_q50_rain30",
    # "d230_s80_NORAIN",
    # "model_trigger",
]
df_im = df_plot[["nameyear"] + plot_cols].set_index("nameyear").T
numeric_map = df_im.map(lambda x: 1 if x else 0)
cmap = ListedColormap(["lightgrey", "red"])

fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
ax.imshow(numeric_map, cmap=cmap, interpolation="nearest", aspect=1)

ax.set_xticks([])
ax.set_yticks([])
ax.axis("off")

linewidth = 5
for (i, j), val in np.ndenumerate(numeric_map):
    color = "red" if val == 1 else "lightgrey"
    rect = plt.Rectangle(
        [j - linewidth, i - linewidth],
        1,
        1,
        edgecolor="white",
        facecolor=color,
        lw=linewidth,
    )
    ax.add_patch(rect)

# Set the axis limits to fit the size of the matrix
ax.set_xlim(-linewidth, numeric_map.shape[1] - linewidth)
ax.set_ylim(numeric_map.shape[0] - linewidth, -linewidth)
ax.invert_xaxis()
```

```python
trigger_str = "d230_s50_AND_max_roll2_sum_rain40"
trigger_list = df_im.T
trigger_list = trigger_list[trigger_list[trigger_str]]
trigger_list
```

```python
triggers[triggers[trigger_str]][["sid", "name", trigger_str]]
```

```python

```

```python
# 4 year RP
plot_cols = [
    "d230_s70_AND_max_mean_rain30",
    # "d230_s50_AND_max_mean_rain30",
    # "d240_s40_AND_max_q50_rain30",
    # "d230_s80_NORAIN",
    # "model_trigger",
]
df_im = df_plot[["nameyear"] + plot_cols].set_index("nameyear").T
numeric_map = df_im.map(lambda x: 1 if x else 0)
cmap = ListedColormap(["lightgrey", "red"])

fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
ax.imshow(numeric_map, cmap=cmap, interpolation="nearest", aspect=1)

ax.set_xticks([])
ax.set_yticks([])
ax.axis("off")

linewidth = 5
for (i, j), val in np.ndenumerate(numeric_map):
    color = "red" if val == 1 else "lightgrey"
    rect = plt.Rectangle(
        [j - linewidth, i - linewidth],
        1,
        1,
        edgecolor="white",
        facecolor=color,
        lw=linewidth,
    )
    ax.add_patch(rect)

# Set the axis limits to fit the size of the matrix
ax.set_xlim(-linewidth, numeric_map.shape[1] - linewidth)
ax.set_ylim(numeric_map.shape[0] - linewidth, -linewidth)
ax.invert_xaxis()
```

```python
# 5 year RP
plot_cols = [
    "d230_s80_AND_max_mean_rain30",
    # "d230_s50_AND_max_mean_rain30",
    # "d240_s40_AND_max_q50_rain30",
    # "d230_s80_NORAIN",
    # "model_trigger",
]
df_im = df_plot[["nameyear"] + plot_cols].set_index("nameyear").T
numeric_map = df_im.map(lambda x: 1 if x else 0)
cmap = ListedColormap(["lightgrey", "red"])

fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
ax.imshow(numeric_map, cmap=cmap, interpolation="nearest", aspect=1)

ax.set_xticks([])
ax.set_yticks([])
ax.axis("off")

linewidth = 5
for (i, j), val in np.ndenumerate(numeric_map):
    color = "red" if val == 1 else "lightgrey"
    rect = plt.Rectangle(
        [j - linewidth, i - linewidth],
        1,
        1,
        edgecolor="white",
        facecolor=color,
        lw=linewidth,
    )
    ax.add_patch(rect)

# Set the axis limits to fit the size of the matrix
ax.set_xlim(-linewidth, numeric_map.shape[1] - linewidth)
ax.set_ylim(numeric_map.shape[0] - linewidth, -linewidth)
ax.invert_xaxis()
```

```python
plot_cols = [
    "d230_s80_AND_max_mean_rain30",
    # "d230_s70_AND_max_mean_rain30",
    # "d220_s50_AND_max_mean_rain30",
    # "d220_s80_NORAIN",
    # "d220_s50_AND_max_q90_rain50",
    # "d220_s50_AND_max_q70_rain40",
    # "d220_s50_AND_max_q60_rain30",
    # "d230_s50_AND_max_mean_rain30",
    # "d230_s50_AND_max_q90_rain50",
    # "d240_s40_AND_max_q50_rain30",
]
df_im = df_plot[["nameyear"] + plot_cols].set_index("nameyear").T
numeric_map = df_im.map(lambda x: 1 if x else 0)
cmap = ListedColormap(["lightgrey", "red"])

fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
ax.imshow(numeric_map, cmap=cmap, interpolation="nearest", aspect=1)
ax.invert_xaxis()
ax.set_xticks([])
ax.set_yticks([])
# ax.axis("off")
# ax.yticks(ticks=range(len(numeric_map.index)), labels=numeric_map.index)
ax.set_xticks(
    ticks=range(len(numeric_map.columns)),
    labels=numeric_map.columns,
    rotation=90,
)
```

```python

```
