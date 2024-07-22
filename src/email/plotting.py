import io
import json
from typing import Literal

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytz

from src.constants import FRENCH_MONTHS, LON_ZOOM_RANGE
from src.datasources import codab, nhc
from src.email.utils import (
    TEST_MONITOR_ID,
    add_test_row_to_monitoring,
    open_static_image,
)
from src.monitoring import monitoring_utils
from src.utils import blob


def get_plot_blob_name(monitor_id, plot_type: Literal["map"]):
    return f"{blob.PROJECT_PREFIX}/plots/fcast/{monitor_id}_{plot_type}.png"


def convert_validTime_to_fr_str(x):
    fr_str = x.strftime("%Hh%M, %-d %b")
    for en_mo, fr_mo in FRENCH_MONTHS.items():
        fr_str = fr_str.replace(en_mo, fr_mo)
    return fr_str


def update_fcast_plots(clobber: bool = False, verbose: bool = False):
    df_monitoring = monitoring_utils.load_existing_monitoring_points("fcast")
    existing_plot_blobs = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/plots/fcast/"
    )

    for monitor_id, row in df_monitoring.set_index("monitor_id").iterrows():
        for plot_type in ["map"]:
            blob_name = get_plot_blob_name(monitor_id, plot_type)
            if blob_name in existing_plot_blobs and not clobber:
                if verbose:
                    print(f"Skipping {blob_name}, already exists")
                continue
            print(f"Creating {blob_name}")
            create_fcast_plot(monitor_id, plot_type)


def create_fcast_plot(monitor_id, plot_type: Literal["map"]):
    adm = codab.load_codab_from_blob(admin_level=0)
    trig_zone = codab.load_buffer()
    if plot_type == "map":
        lts = {
            "action": {
                "color": "darkorange",
                "plot_color": "black",
                "dash": "solid",
                "label": "Action",
                "zorder": 2,
                "lt_max": pd.Timedelta(days=3),
                "lt_min": pd.Timedelta(days=-1),
                "threshs": {
                    "roll2_rain_dist": 42,
                    "wind_dist": 64,
                    "dist_min": 230,
                },
            },
            "readiness": {
                "color": "dodgerblue",
                "plot_color": "grey",
                "dash": "dot",
                "label": "Mobilisation",
                "zorder": 1,
                "lt_max": pd.Timedelta(days=5),
                "lt_min": pd.Timedelta(days=2),
                "threshs": {
                    "roll2_rain_dist": 42,
                    "wind_dist": 64,
                    "dist_min": 230,
                },
            },
        }
        df_monitoring = monitoring_utils.load_existing_monitoring_points(
            "fcast"
        )
        if monitor_id == TEST_MONITOR_ID:
            df_monitoring = add_test_row_to_monitoring(df_monitoring, "fcast")
        monitoring_point = df_monitoring.set_index("monitor_id").loc[
            monitor_id
        ]
        haiti_tz = pytz.timezone("America/Port-au-Prince")
        cyclone_name = monitoring_point["name"]
        atcf_id = monitoring_point["atcf_id"]
        if atcf_id == "TEST_ATCF_ID":
            atcf_id = "al022024"
        issue_time = monitoring_point["issue_time"]
        issue_time_hti = issue_time.astimezone(haiti_tz)

        df_tracks = nhc.load_recent_glb_forecasts()
        tracks_f = df_tracks[
            (df_tracks["id"] == atcf_id)
            & (df_tracks["issuance"] == issue_time)
        ].copy()
        tracks_f["validTime_hti"] = tracks_f["validTime"].apply(
            lambda x: x.astimezone(haiti_tz)
        )
        tracks_f["valid_time_str"] = tracks_f["validTime_hti"].apply(
            convert_validTime_to_fr_str
        )

        tracks_f["lt"] = tracks_f["validTime"] - tracks_f["issuance"]
        rain_level = monitoring_point["readiness_p"]
        fig = go.Figure()

        # adm0 outline
        for geom in adm.geometry[0].geoms:
            x, y = geom.exterior.coords.xy
            fig.add_trace(
                go.Scattermapbox(
                    lon=list(x),
                    lat=list(y),
                    mode="lines",
                    line_color="grey",
                    showlegend=False,
                )
            )
        # buffer
        fig.add_trace(
            go.Choroplethmapbox(
                geojson=json.loads(trig_zone.geometry.to_json()),
                locations=trig_zone.index,
                z=[1],
                colorscale="Reds",
                marker_opacity=0.2,
                showscale=False,
                marker_line_width=0,
                hoverinfo="none",
            )
        )

        for lt_name in ["readiness", "action"]:
            lt_params = lts[lt_name]
            dff = tracks_f[
                (tracks_f["lt"] <= lt_params["lt_max"])
                & (tracks_f["lt"] >= lt_params["lt_min"])
            ]
            # triggered points
            dff_trig = dff[
                (dff["maxwind"] >= lt_params["threshs"]["wind_dist"])
                & (dff["lt"] > lt_params["lt_min"])
            ]
            fig.add_trace(
                go.Scattermapbox(
                    lon=dff_trig["longitude"],
                    lat=dff_trig["latitude"],
                    mode="markers",
                    marker=dict(size=50, color="red"),
                )
            )
            # all points
            fig.add_trace(
                go.Scattermapbox(
                    lon=dff["longitude"],
                    lat=dff["latitude"],
                    mode="markers+text+lines",
                    marker=dict(size=40, color=lt_params["plot_color"]),
                    text=dff["maxwind"].astype(str),
                    line=dict(width=2, color=lt_params["plot_color"]),
                    textfont=dict(size=20, color="white"),
                    customdata=dff["valid_time_str"],
                    hovertemplate=(
                        "Heure valide: %{customdata}<extra></extra>"
                    ),
                )
            )

            # rainfall
            if lt_name == "readiness":
                # rain_level = dff["roll2_rain_dist"].max()
                if pd.isnull(rain_level):
                    rain_level_str = ""
                else:
                    rain_level_str = int(rain_level)
                if rain_level > lt_params["threshs"]["roll2_rain_dist"]:
                    fig.add_trace(
                        go.Scattermapbox(
                            lon=[-72.3],
                            lat=[19],
                            mode="markers",
                            marker=dict(size=50, color="red"),
                        )
                    )
                fig.add_trace(
                    go.Scattermapbox(
                        lon=[-72.3],
                        lat=[19],
                        mode="text+markers",
                        text=[rain_level_str],
                        marker=dict(size=40, color="blue"),
                        textfont=dict(size=20, color="white"),
                        hoverinfo="none",
                    )
                )
        adm_centroid = adm.to_crs(3857).centroid.to_crs(4326)[0]
        centroid_lat, centroid_lon = adm_centroid.y, adm_centroid.x
        lat_max = max(tracks_f["latitude"])
        lat_max = max(lat_max, centroid_lat)
        lat_min = min(tracks_f["latitude"])
        lat_min = min(lat_min, centroid_lat)
        lon_max = max(tracks_f["longitude"])
        lon_max = max(lon_max, centroid_lon)
        lon_min = min(tracks_f["longitude"])
        lon_min = min(lon_min, centroid_lon)

        width_to_height = 1
        margin = 1.7
        height = (lat_max - lat_min) * margin * width_to_height
        width = (lon_max - lon_min) * margin
        lon_zoom = np.interp(width, LON_ZOOM_RANGE, range(20, 0, -1))
        lat_zoom = np.interp(height, LON_ZOOM_RANGE, range(20, 0, -1))
        zoom = round(min(lon_zoom, lat_zoom), 2)

        issue_time_str_fr = convert_validTime_to_fr_str(issue_time_hti)
        plot_title = (
            f"Prévision NOAA pour {cyclone_name}<br>"
            f"<sup>Émise {issue_time_str_fr} (heure locale Haïti)</sup>"
        )

        encoded_legend = open_static_image("map_legend.png")

        fig.update_layout(
            title=plot_title,
            mapbox_style="open-street-map",
            mapbox_zoom=zoom,
            mapbox_center_lat=(lat_max + lat_min) / 2,
            mapbox_center_lon=(lon_max + lon_min) / 2,
            margin={"r": 0, "t": 50, "l": 0, "b": 0},
            height=850,
            width=800,
            showlegend=False,
            images=[
                dict(
                    source=f"data:image/png;base64,{encoded_legend}",
                    xref="paper",
                    yref="paper",
                    x=0.01,
                    y=0.01,
                    sizex=0.3,
                    sizey=0.3,
                    xanchor="left",
                    yanchor="bottom",
                    opacity=0.7,
                )
            ],
        )

        buffer = io.BytesIO()
        # scale corresponds to 150 dpi
        fig.write_image(buffer, format="png", scale=2.08)
        buffer.seek(0)

        blob_name = get_plot_blob_name(monitor_id, plot_type)
        blob.upload_blob_data(blob_name, buffer)
