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

# Leadtime cutoff

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from src.datasources import nhc, ibtracs
from src.constants import *
```

```python
OBSV_TRIG_STR = "d230_s50_AND_max_roll2_sum_rain40"
```

```python
sid_atcf = ibtracs.load_ibtracs_sid_atcf_names()
sid_atcf.loc[:, "name"] = sid_atcf["name"].str.capitalize()
sid_atcf.loc[:, "usa_atcf_id"] = sid_atcf["usa_atcf_id"].str.lower()
sid_atcf = sid_atcf.rename(columns={"usa_atcf_id": "atcf_id"})
```

```python
obsv_tracks = ibtracs.load_hti_distances()
obsv_tracks = obsv_tracks.merge(sid_atcf[["sid", "atcf_id"]], on="sid")
```

```python
trigger_str = "d230_s50_AND_max_roll2_sum_rain40"
filename = f"{trigger_str}_triggers.csv"
obsv_triggers = pd.read_csv(ibtracs.IBTRACS_HTI_PROC_DIR / filename)
obsv_triggers = obsv_triggers.merge(sid_atcf[["sid", "atcf_id"]])
obsv_triggers = obsv_triggers[obsv_triggers["year"] != 2020]


def closest_date(sid):
    dff = obsv_tracks[obsv_tracks["sid"] == sid]
    dff = dff[dff["distance (m)"] == dff["distance (m)"].min()]
    return dff["time"].min()


obsv_triggers["closest_time"] = obsv_triggers["sid"].apply(closest_date)
```

```python
obsv_triggers["year"].unique()
```

```python
# nhc.calculate_hist_fcast_monitors(lt_cutoff_hrs=48)
```

```python
# nhc.calculate_hist_fcast_monitors(lt_cutoff_hrs=36)
```

```python
monitors = nhc.load_hist_fcast_monitors(lt_cutoff_hrs=36)
```

```python
monitors[monitors["issue_time"].dt.year == 2020]
```

```python
def determine_triggers(lt_threshs, monitors):
    dicts = []
    for atcf_id, group in monitors.groupby("atcf_id"):
        for lt_name, threshs in lt_threshs.items():
            dff = group[group["lt_name"] == lt_name]
            dff_p = dff[dff["roll2_rain_dist"] >= threshs.get("p")]
            dff_s = dff[dff["wind_dist"] >= threshs.get("s")]

            if not dff_p.empty and not dff_s.empty:
                pass

            dff_both = dff[
                (dff["roll2_rain_dist"] >= threshs.get("p"))
                & (dff["wind_dist"] >= threshs.get("s"))
            ]
            if not dff_both.empty:
                trig_date = dff_both["issue_time"].min()
                dicts.append(
                    {
                        "atcf_id": atcf_id,
                        "trig_date": trig_date,
                        "lt_name": lt_name,
                        "trig_type": "simul",
                    }
                )

    triggers = pd.DataFrame(dicts)

    triggers = (
        triggers.pivot(
            index=["atcf_id", "trig_type"],
            columns="lt_name",
            values="trig_date",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    triggers = triggers.merge(
        obsv_triggers[obsv_triggers["atcf_id"] != LAURA_ATCF_ID][
            [
                "atcf_id",
                "closest_time",
                "target",
                "affected_population",
                OBSV_TRIG_STR,
            ]
        ],
        how="outer",
    )
    triggers = triggers.merge(sid_atcf, how="left")

    for lt_name in lt_threshs:
        triggers[f"{lt_name}_lt"] = (
            triggers["closest_time"] - triggers[lt_name]
        )
        triggers[f"FN_{lt_name}"] = (
            triggers[lt_name].isna() & triggers["target"]
        )
        triggers[f"FP_{lt_name}"] = (
            ~triggers[lt_name].isna() & ~triggers["target"]
        )
    triggers = triggers.sort_values("affected_population", ascending=False)
    return triggers
```

```python
lt_threshs = {
    "readiness": {"p": 35, "s": 34},
    "action": {"p": 42, "s": 64},
}
triggers = determine_triggers(lt_threshs, monitors)
```

```python
triggers[~triggers["action"].isnull()]
```

```python
triggers["action_lt"].mean()
```

```python
lt_threshs = {
    "readiness": {"p": 35, "s": 34},
    "action": {"p": 42, "s": 64},
}
triggers_cutoff = determine_triggers(
    lt_threshs, monitors[~monitors["past_cutoff"]]
)
```

```python
triggers_cutoff[~triggers_cutoff["readiness"].isnull()]
```

```python
triggers_cutoff["action_lt"].mean()
```

```python
df_plot = triggers_cutoff.rename(columns={OBSV_TRIG_STR: "obsv"})
df_plot["obsv"] = df_plot["obsv"].astype(bool)
df_plot["action"] = ~df_plot["action"].isnull()
df_plot["readiness"] = ~df_plot["readiness"].isnull()
df_plot["year"] = df_plot["atcf_id"].str[-4:].astype(int)
df_plot["nameyear"] = df_plot["name"] + " " + df_plot["year"].astype(str)
df_plot["action|obsv"] = df_plot["action"] | df_plot["obsv"]
df_plot["any"] = df_plot["action|obsv"] | df_plot["readiness"]
df_plot["affected_population"] = (
    df_plot["affected_population"].fillna(0).astype(int)
)
df_plot = df_plot.sort_values(["affected_population"] + cols, ascending=False)
```

```python
total_years = df_plot["year"].nunique()
print(total_years)
for col in ["readiness", "action", "obsv", "action|obsv", "any"]:
    n_years = df_plot[df_plot[col]]["year"].nunique()
    print(f"{col}: {n_years}")
```

```python
def highlight_true(val):
    if isinstance(val, bool) and val is True:
        return "background-color: crimson"
    return ""


cols = ["readiness", "action", "obsv", "affected_population"]
df_plot.set_index("nameyear")[cols].style.bar(
    subset="affected_population",
    color="dodgerblue",
    vmax=500000,
    props="width: 200px;",
).map(highlight_true)
```

```python
df_plot
```
