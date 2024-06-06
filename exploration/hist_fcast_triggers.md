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

# Historical forecast triggers

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from tqdm.notebook import tqdm

from src.datasources import nhc, codab, chirps_gefs, chirps, ibtracs
from src.utils import blob
from src.constants import *
```

```python
monitors = nhc.load_hist_fcast_monitors()
monitors = monitors[monitors["issue_time"].dt.year != 2020]
```

```python
monitors
```

```python
monitors["atcf_id"].nunique()
```

```python
storms = monitors.groupby("atcf_id")["name"].first().reset_index()
storms["year"] = storms["atcf_id"].str[-4:].astype(int)
storms = storms.sort_values(["year", "name"], ascending=False)
storms["nameyear"] = storms["name"] + " " + storms["year"].astype(str)
storms = storms.set_index("atcf_id")
storms
```

```python
D_THRESH = 230
P_THRESH = 40
S_THRESH = 50
```

```python
sid_atcf = ibtracs.load_ibtracs_sid_atcf_names()
sid_atcf.loc[:, "name"] = sid_atcf["name"].str.capitalize()
sid_atcf.loc[:, "usa_atcf_id"] = sid_atcf["usa_atcf_id"].str.lower()
sid_atcf = sid_atcf.rename(columns={"usa_atcf_id": "atcf_id"})
```

```python
sid_atcf
```

```python
obsv_tracks = ibtracs.load_hti_distances()
obsv_tracks = obsv_tracks.merge(sid_atcf[["sid", "atcf_id"]], on="sid")
```

```python
obsv_tracks
```

```python
trigger_str = "d230_s50_AND_max_roll2_sum_rain40"
filename = f"{trigger_str}_triggers.csv"
obsv_triggers = pd.read_csv(ibtracs.IBTRACS_HTI_PROC_DIR / filename)
obsv_triggers = obsv_triggers.merge(sid_atcf[["sid", "atcf_id"]])
obsv_triggers = obsv_triggers[obsv_triggers["year"] != 2020]
obsv_triggers
```

```python
def closest_date(sid):
    dff = obsv_tracks[obsv_tracks["sid"] == sid]
    dff = dff[dff["distance (m)"] == dff["distance (m)"].min()]
    return dff["time"].min()


obsv_triggers["closest_time"] = obsv_triggers["sid"].apply(closest_date)
```

```python
def determine_triggers(lt_threshs):
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
            ["atcf_id", "closest_time", "target", "affected_population"]
        ],
        how="outer",
    )
    triggers = triggers.merge(sid_atcf, how="left")
    triggers["target"] = triggers["target"].astype(bool)

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
    return triggers
```

```python
ps = np.arange(30, 60)
ss = np.arange(34, 70)

N_YEARS = 23
dicts = []
for lt_name in ["readiness", "action"]:
    for p in tqdm(ps):
        for s in ss:
            lt_threshs = {
                lt_name: {"p": p, "s": s},
            }
            df_in = determine_triggers(lt_threshs)
            df_in["year"] = df_in["atcf_id"].str[-4:].astype(int)
            dicts.append(
                {
                    "p": p,
                    "s": s,
                    "lt_name": lt_name,
                    "rp": N_YEARS / df_in[lt_name].dt.year.nunique(),
                    "fn": df_in[f"FN_{lt_name}"].sum(),
                    "fp": df_in[f"FP_{lt_name}"].sum(),
                    "lt": df_in[df_in["target"]][f"{lt_name}_lt"].mean(),
                    "affected_population": df_in[~df_in[lt_name].isnull()][
                        "affected_population"
                    ].sum(),
                }
            )

metrics = pd.DataFrame(dicts)
metrics["lt_hours"] = metrics["lt"].dt.total_seconds() / 3600
```

```python
for lt_name, (rp_min, rp_max) in [
    ("readiness", (1.5, 2.8)),
    ("action", (2.8, 3.5)),
]:
    for var in ["fn", "fp", "lt_hours", "rp", "affected_population"]:
        pivot_df = metrics[
            (metrics["lt_name"] == lt_name)
            & (metrics["rp"] >= rp_min)
            & (metrics["rp"] <= rp_max)
        ].pivot(index="s", columns="p", values=var)

        # Create the heatmap
        plt.figure(figsize=(10, 8))
        ax = sns.heatmap(
            pivot_df, annot=True, cmap="coolwarm"
        )  # 'annot=True' displays the values
        ax.invert_yaxis()  # Invert the y-axis
        plt.title(f"{lt_name} {var}")
        plt.show()
```

```python
lt_threshs = {
    "readiness": {"p": 35, "s": 34},
    "action": {"p": 42, "s": 64},
}
triggers = determine_triggers(lt_threshs)
trig_str = (
    f'triggers_r_p{lt_threshs["readiness"]["p"]}_s{lt_threshs["readiness"]["s"]}_'
    f'a_p{lt_threshs["action"]["p"]}_s{lt_threshs["action"]["s"]}'
)
print(trig_str)
blob_name = f"{blob.PROJECT_PREFIX}/processed/{trig_str}.csv"
triggers = triggers.sort_values("affected_population", ascending=False)
blob.upload_blob_data(blob_name, triggers.to_csv(), prod_dev="dev")
```

```python
blob.upload_parquet_to_blob(blob_name, triggers, prod_dev="dev")
```

```python
lt_threshs = {
    "readiness": {"p": 35, "s": 34},
    "action": {"p": 42, "s": 64},
}
trig_str = (
    f'triggers_r_p{lt_threshs["readiness"]["p"]}_s{lt_threshs["readiness"]["s"]}_'
    f'a_p{lt_threshs["action"]["p"]}_s{lt_threshs["action"]["s"]}'
)
print(trig_str)
blob_name = f"{blob.PROJECT_PREFIX}/processed/{trig_str}.csv"
```

```python
triggers = blob.load_parquet_from_blob(blob_name, prod_dev="dev")
triggers
```

```python
triggers.dtypes
```

```python
def plot_forecasts(atcf_id):
    d_lim = 600
    lts = {
        "action": {
            "color": "darkorange",
            "dash": "solid",
            "label": "Action",
            "threshs": {
                "roll2_rain_dist": 42,
                "wind_dist": 64,
                "dist_min": D_THRESH,
            },
        },
        "readiness": {
            "color": "dodgerblue",
            "dash": "dot",
            "label": "Mobilisation",
            "threshs": {
                "roll2_rain_dist": 35,
                "wind_dist": 34,
                "dist_min": D_THRESH,
            },
        },
    }

    df_storm = monitors.set_index("atcf_id").loc[atcf_id].reset_index()
    df_storm = df_storm[df_storm["dist_min"] < d_lim]

    df_storm["issue_time_str"] = df_storm["issue_time"].dt.strftime(
        "%Hh, %d %b"
    )
    name = df_storm.iloc[0]["name"]

    closest_time = triggers.set_index("atcf_id").loc[atcf_id]["closest_time"]
    min_time = df_storm["issue_time"].min()
    s_max = df_storm["wind_dist"].max()
    r_max = df_storm["roll2_rain_dist"].max()

    readiness_time = triggers.set_index("atcf_id").loc[atcf_id]["readiness"]
    action_time = triggers.set_index("atcf_id").loc[atcf_id]["action"]

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True)
    shapes = []
    annotations = []
    prev_trig_time = pd.NaT
    for lt_name, params in lts.items():
        trig_time, trig_lt = triggers.set_index("atcf_id").loc[atcf_id][
            [lt_name, f"{lt_name}_lt"]
        ]
        if prev_trig_time - trig_time < pd.Timedelta(days=1):
            offset = -0.2
            standoff = 80
        else:
            offset = -0.1
            standoff = 50
        if not pd.isnull(trig_time):
            shapes.append(
                {
                    "type": "line",
                    "xref": "x3",
                    "yref": "paper",
                    "x0": trig_time,
                    "x1": trig_time,
                    "y0": offset,
                    "y1": 1,
                    "line": {
                        "color": params.get("color"),
                        "width": 1,
                        "dash": "solid",
                    },
                }
            )
            annotations.append(
                {
                    "x": trig_time,
                    "y": offset,
                    "xref": "x3",
                    "yref": "paper",
                    "text": (
                        f'{params.get("label")} :<br>'
                        f'{trig_time.strftime("%-d %b, %-H:%M")}<br>'
                        f"(préavis {trig_lt.total_seconds() / 3600:.0f} heures)"
                    ),
                    "showarrow": False,
                    "xanchor": "center",
                    "yanchor": "top",
                    "font": {"size": 10, "color": params.get("color")},
                }
            )

        df_plot = df_storm[df_storm["lt_name"] == lt_name]
        for j, var in enumerate(["wind_dist", "roll2_rain_dist", "dist_min"]):
            mode = "markers" if len(df_plot[var].dropna()) == 1 else "lines"
            fig.add_trace(
                go.Scatter(
                    x=df_plot["issue_time"],
                    y=df_plot[var],
                    mode=mode,
                    name=params["label"],
                    line_color=params.get("color"),
                    line_dash=params.get("dash"),
                    line_width=3,
                ),
                row=j + 1,
                col=1,
            )
            shapes.append(
                {
                    "type": "line",
                    "xref": "paper",
                    "yref": f"y{j+1}",
                    "x0": 0,
                    "x1": 1,
                    "y0": params["threshs"][var],
                    "y1": params["threshs"][var],
                    "line": {
                        "color": params["color"],
                        "width": 1,
                        "dash": "dash",
                    },
                }
            )
        prev_trig_time = trig_time

    # zero lines
    shapes.extend(
        [
            {
                "type": "line",
                "xref": "paper",
                "yref": f"y{x}",
                "x0": 0,
                "x1": 1,
                "y0": 0,
                "y1": 0,
                "line": {"color": "black", "width": 2},
            }
            for x in [1, 2]
        ]
    )

    # closest pass
    shapes.append(
        {
            "type": "line",
            "xref": "x3",
            "yref": "paper",
            "x0": closest_time,
            "x1": closest_time,
            "y0": -0.1,
            "y1": 1,
            "line": {"color": "black", "width": 2, "dash": "solid"},
        }
    )
    annotations.append(
        {
            "x": closest_time,
            "y": -0.1,
            "xref": "x3",
            "yref": "paper",
            "text": f'Passage plus proche :<br>{closest_time.strftime("%-d %b, %H:%M")}',
            "showarrow": False,
            "xanchor": "center",
            "yanchor": "top",
            "font": {"size": 10, "color": "black"},
        }
    )

    yaxis_font_size = 14
    fig.update_traces(xaxis="x3")
    fig.update_layout(
        hovermode="x unified",
        title=f"Prévisions de ouragan {name}<br><sup>"
        "Graphiques indiquent valeurs prévues; "
        "lignes en tirets indiquent seuils</sup>",
        yaxis=dict(
            title=dict(
                text="Vitesse<br>de vent<br>(noeuds)",
                font_size=yaxis_font_size,
            ),
            range=[0, s_max + 10],
        ),
        yaxis2=dict(
            title=dict(
                text="Précipitations<br>sur 2 jours<br>(mm)",
                font_size=yaxis_font_size,
            ),
            range=[0, r_max + 10],
        ),
        yaxis3=dict(
            title=dict(
                text="Distance<br>minimum<br>(km)", font_size=yaxis_font_size
            ),
            range=[0, d_lim],
        ),
        xaxis3=dict(
            title=dict(
                text="Heure de publication de prévision", standoff=standoff
            ),
            range=[min_time, closest_time + pd.Timedelta(days=0.5)],
        ),
        shapes=shapes,
        annotations=annotations,
        template="simple_white",
        height=630,
        width=800,
        showlegend=False,
        margin={"b": 100},
    )
    return fig
```

```python
for atcf_id in triggers[triggers["target"]].iloc[:1]["atcf_id"]:
    try:
        plot_forecasts(atcf_id).show()
    except:
        print(atcf_id)
```

```python
# Ike
monitors.set_index("atcf_id").loc["al092008"]
```

```python
# Irma
monitors.set_index("atcf_id").loc["al112017"].dropna()
```

```python
# Sandy
monitors.set_index("atcf_id").loc["al182012"].dropna()
```

```python
ax = (
    obsv_tracks.set_index("atcf_id")
    .loc["al182012"]
    .plot(x="time", y="distance (m)")
)
ax.axhline(y=230 * 1e3, color="lightgray", linestyle="dashed")
```

```python
# Laura
monitors.set_index("atcf_id").loc["al132020"].iloc[:60]
```

```python
ax = (
    obsv_tracks.set_index("atcf_id")
    .loc["al132020"]
    .plot(x="time", y="distance (m)")
)
ax.axhline(y=230 * 1e3, color="lightgray", linestyle="dashed")
```
