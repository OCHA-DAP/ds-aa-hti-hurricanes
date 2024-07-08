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

# Three-stage trigger

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from src.datasources import nhc, chirps, chirps_gefs, ibtracs, impact
from src.utils import blob
```

```python
D_THRESH = 230
```

```python
sid_atcf = ibtracs.load_ibtracs_sid_atcf_names()
sid_atcf.loc[:, "name"] = sid_atcf["name"].str.capitalize()
sid_atcf.loc[:, "usa_atcf_id"] = sid_atcf["usa_atcf_id"].str.lower()
sid_atcf = sid_atcf.rename(columns={"usa_atcf_id": "atcf_id"})
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
sid_atcf
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/stats_{D_THRESH}km.csv"
stats = blob.load_csv_from_blob(blob_name)
stats = stats[
    [
        "sid",
        "max_wind",
        "max_roll2_sum_rain_imerg",
    ]
]
```

```python
stats
```

```python
monitors = nhc.load_hist_fcast_monitors(lt_cutoff_hrs=36)
```

```python
monitors
```

```python
monitors[monitors["name"] == "Laura"].dropna(subset="wind_dist")
```

```python
def determine_triggers(lt_threshs, obsv_threshs):
    dicts = []
    for atcf_id, group in monitors[~monitors["past_cutoff"]].groupby(
        "atcf_id"
    ):
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
                        "lt_name": f"{lt_name}_date",
                    }
                )
    fcast_triggers = pd.DataFrame(dicts)

    fcast_triggers = (
        fcast_triggers.pivot(
            index="atcf_id",
            columns="lt_name",
            values="trig_date",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    fcast_triggers = fcast_triggers.merge(sid_atcf).drop(
        columns=["atcf_id", "name"]
    )
    stats["obsv"] = (stats["max_wind"] >= obsv_threshs.get("s")) & (
        stats["max_roll2_sum_rain_imerg"] >= obsv_threshs.get("p")
    )

    triggers = (
        stats.merge(fcast_triggers, how="outer", on="sid")
        .merge(affected, how="left")
        .merge(sid_atcf)
    )

    for lt_name in ["readiness", "action"]:
        triggers[lt_name] = ~triggers[f"{lt_name}_date"].isnull()

    triggers = triggers.sort_values("affected_population", ascending=False)
    triggers["obsv"] = triggers["obsv"].fillna(False)
    triggers["year"] = triggers["atcf_id"].str[-4:]
    triggers["nameyear"] = triggers["name"] + " " + triggers["year"]

    cols = [
        "readiness",
        "action",
        "obsv",
        "affected_population",
        "nameyear",
        "year",
    ]
    df_plot = triggers[cols]
    df_plot = df_plot.set_index("nameyear")
    df_plot["affected_population"] = (
        df_plot["affected_population"].fillna(0).astype(int)
    )
    df_plot["action|obsv"] = df_plot["action"] | df_plot["obsv"]
    df_plot["any"] = df_plot["action|obsv"] | df_plot["readiness"]

    for col in ["readiness", "action", "obsv", "action|obsv", "any"]:
        n_years = df_plot[df_plot[col]]["year"].nunique()
        print(f"{col}: {24 / n_years:.1f} years")

    def highlight_true(val):
        if isinstance(val, bool) and val is True:
            return "background-color: crimson"
        return ""

    cols = ["readiness", "action", "obsv", "affected_population"]
    display(
        df_plot[cols]
        .style.bar(
            subset="affected_population",
            color="dodgerblue",
            vmax=500000,
            props="width: 300px;",
        )
        .map(highlight_true)
        .set_table_styles(
            {
                "affected_population": [
                    {"selector": "th", "props": [("text-align", "left")]},
                    {"selector": "td", "props": [("text-align", "left")]},
                ]
            }
        )
        .format({"affected_population": "{:,}"})
    )
```

```python
# option 1
lt_threshs = {
    # "readiness": {"p": 35, "s": 34, "lt_days": 5},
    "readiness": {"p": 42, "s": 64, "lt_days": 5},
    "action": {"p": 42, "s": 64, "lt_days": 3},
}
# 3yr RP
# obsv_threshs = {"p": 60, "s": 50}
# 4yr RP
# obsv_threshs = {"p": 70, "s": 50}
# 5yr RP
obsv_threshs = {"p": 60, "s": 70}
determine_triggers(lt_threshs, obsv_threshs)
```

```python
# option 2
lt_threshs = {
    "readiness": {"p": 55, "s": 64, "lt_days": 5},
    "action": {"p": 54, "s": 64, "lt_days": 3},
}
# 3yr RP
# obsv_threshs = {"p": 60, "s": 50}
# 4yr RP
obsv_threshs = {"p": 70, "s": 50}
# 5yr RP
# obsv_threshs = {"p": 60, "s": 70}
determine_triggers(lt_threshs, obsv_threshs)
```

```python
# option 3
lt_threshs = {
    "readiness": {"p": 56, "s": 61, "lt_days": 5},
    "action": {"p": 42, "s": 64, "lt_days": 3},
}
# 3yr RP
# obsv_threshs = {"p": 60, "s": 50}
# 4yr RP
obsv_threshs = {"p": 70, "s": 50}
# 5yr RP
# obsv_threshs = {"p": 60, "s": 70}
determine_triggers(lt_threshs, obsv_threshs)
```

```python
# option 4
lt_threshs = {
    # "readiness": {"p": 35, "s": 34, "lt_days": 5},
    "readiness": {"p": 42, "s": 64, "lt_days": 5},
    "action": {"p": 42, "s": 64, "lt_days": 3},
}
# 3yr RP
# obsv_threshs = {"p": 60, "s": 50}
# 4yr RP
obsv_threshs = {"p": 70, "s": 50}
# 5yr RP
# obsv_threshs = {"p": 60, "s": 70}
determine_triggers(lt_threshs, obsv_threshs)
```

```python

```
