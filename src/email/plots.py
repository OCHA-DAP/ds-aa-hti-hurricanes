"""Email figures: WSP exceedance chart and storm map.

Matplotlib only, rendered to PNG and returned as base64 <img> tags
(swapped for Listmonk media URLs at send time — see send.py). Style
tokens follow ds-storms-alerts (HDX v2).
"""

import base64
import html as _html
import io
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from src.constants import FRENCH_MONTHS

# ── style tokens (ds-storms-alerts / HDX v2) ─────────────────────────────
INK = "#1f2324"
INK_2 = "#5e6a6b"
INK_3 = "#7e8e8f"
LINE = "#e2e8e8"
GREY_FILL = "#d8e0e1"
GREY_EDGE = "#9db1b3"
WIND_COLORS = {34: "#d48f2a", 50: "#d06a5e", 64: "#9d372b"}
WSP_BLUE = "#1862d8"
FONTS = ["Roboto", "Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"]

plt.rcParams["font.family"] = FONTS

_EMAIL_CONTENT_WIDTH_PX = 680

_DATA_DIR = Path(__file__).parents[2] / "data"

# WSP percentage is a band's LOWER edge; midpoints per ds-storms-alerts.
WSP_BAND_MIDPOINT = {
    0: 0.025,
    5: 0.075,
    10: 0.15,
    20: 0.25,
    30: 0.35,
    40: 0.45,
    50: 0.55,
    60: 0.65,
    70: 0.75,
    80: 0.85,
    90: 0.95,
}


def fr_datetime(ts: pd.Timestamp) -> str:
    """'25 oct. 2025 15h00' in Haiti local time."""
    ts_hti = ts.tz_localize("UTC") if ts.tzinfo is None else ts
    ts_hti = ts_hti.tz_convert("America/Port-au-Prince")
    out = ts_hti.strftime("%-d %b %Y %Hh%M")
    for en, fr in FRENCH_MONTHS.items():
        out = out.replace(en, fr)
    return out


def fmt_pop(x: float) -> str:
    if x >= 1_000_000:
        return f"{x / 1_000_000:.1f}M".replace(".", ",")
    if x >= 1_000:
        return f"{x / 1_000:.0f}k"
    return f"{x:.0f}"


def fig_to_img_tag(fig, alt: str = "", dpi: int = 200) -> str:
    """PNG at 2x for retina; explicit width/height for Outlook."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    png = buf.getvalue()
    from PIL import Image

    nat_w, nat_h = Image.open(io.BytesIO(png)).size
    w = min(round(nat_w / 2), _EMAIL_CONTENT_WIDTH_PX)
    h = max(1, round(w * nat_h / nat_w))
    img_b64 = base64.b64encode(png).decode("utf-8")
    style = (
        f"width:{w}px;max-width:100%;height:auto;display:block;"
        "margin-bottom:8px"
    )
    return (
        f'<img src="data:image/png;base64,{img_b64}" '
        f'width="{w}" height="{h}" '
        f'alt="{_html.escape(alt, quote=True)}" style="{style}">'
    )


def _exceedance_curve(bands: list[tuple[int, int]], floor: float):
    """Exceedance function of total exposure under the comonotone
    one-severity-draw model (ds-storms-alerts WspPdf).

    bands: (percentage lower-edge, pop_exposed) per band.
    floor: already-observed exposure (P=1 up to the floor).
    Returns (xs, ps) step-curve vertices: P(exposure >= x).
    """
    probs = sorted(
        (WSP_BAND_MIDPOINT.get(int(p), 0.025), int(pop)) for p, pop in bands
    )
    # cumulative population from most-probable band down
    xs, ps = [0.0, float(floor)], [1.0, 1.0]
    cum = float(floor)
    for p_mid, pop in sorted(probs, reverse=True):
        if pop <= 0:
            continue
        cum += pop
        xs.append(cum)
        ps.append(p_mid)
    return np.array(xs), np.array(ps)


def wsp_exceedance_img(
    df_wsp: pd.DataFrame,
    storm_name: str,
    issued_time: pd.Timestamp,
    obsv_floor_by_kt: dict | None = None,
) -> str:
    """One panel: P(population exposée >= x) for each wind level.

    df_wsp: [wind_threshold_kt, percentage, pop_exposed] for one issuance
    (see storms_db.fetch_wsp_exposure).
    """
    obsv_floor_by_kt = obsv_floor_by_kt or {}
    fig, ax = plt.subplots(figsize=(6.8, 3.4), dpi=200)

    x_max = 0.0
    for kt in (34, 50, 64):
        sub = df_wsp[df_wsp["wind_threshold_kt"] == kt]
        floor = float(obsv_floor_by_kt.get(kt, 0))
        bands = list(zip(sub["percentage"], sub["pop_exposed"]))
        if not bands and floor == 0:
            # still draw the zero line so the level shows in the legend
            bands = []
        xs, ps = _exceedance_curve(bands, floor)
        x_max = max(x_max, xs[-1] if len(xs) else 0)
        ax.step(
            xs,
            ps * 100,
            where="post",
            color=WIND_COLORS[kt],
            lw=2.0,
            label=f"vents ≥ {kt} kt",
            zorder=3,
        )

    if x_max == 0:
        x_max = 1000
    ax.set_xlim(0, x_max * 1.06)
    ax.set_ylim(0, 104)
    ax.axhline(50, color=GREY_EDGE, lw=0.8, ls=(0, (2, 3)), zorder=1)
    ax.text(
        x_max * 1.05,
        52,
        "50 %",
        color=INK_3,
        fontsize=8,
        ha="right",
        va="bottom",
    )

    ax.set_xlabel("Population exposée", color=INK_2, fontsize=9)
    ax.set_ylabel(
        "Probabilité d'atteindre\nou dépasser (%)", color=INK_2, fontsize=9
    )
    ax.xaxis.set_major_formatter(lambda x, _: fmt_pop(x))
    ax.tick_params(colors=INK_2, labelsize=8)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(LINE)
        ax.spines[spine].set_linewidth(1.2)
    ax.legend(
        frameon=False,
        fontsize=8.5,
        labelcolor=INK,
        loc="upper right",
        bbox_to_anchor=(1.0, 0.95),
    )
    ax.set_title(
        "Probabilité que la population exposée atteigne un niveau donné\n"
        f"(NHC Wind Speed Probabilities, prévision du "
        f"{fr_datetime(issued_time)})",
        fontsize=9.5,
        color=INK,
        loc="left",
        pad=10,
    )
    return fig_to_img_tag(
        fig,
        alt=f"Distribution probabiliste de l'exposition, {storm_name}",
    )


def _start_map():
    """Figure with the Natural Earth background."""
    import geopandas as gpd

    ne = gpd.read_parquet(_DATA_DIR / "ne110m_countries.parquet")
    fig, ax = plt.subplots(figsize=(6.8, 4.6), dpi=200)
    ax.set_aspect("equal")
    ne.plot(ax=ax, color="#f4f6f6", edgecolor=GREY_EDGE, lw=0.5, zorder=1)
    return fig, ax


def _finish_map(
    fig,
    ax,
    tracks_obsv,
    tracks_fcast,
    adm0,
    title: str,
    layer_handles: list,
    layer_legend_title: str,
):
    """Haiti outline, tracks, framing, legends, title (shared by maps)."""
    # Haiti (thin muted outline so the black track stays unambiguous)
    adm0.boundary.plot(ax=ax, color=INK_2, lw=0.9, zorder=4)

    if tracks_obsv is not None and not tracks_obsv.empty:
        ax.plot(
            tracks_obsv.geometry.x,
            tracks_obsv.geometry.y,
            color=INK,
            lw=1.7,
            zorder=5,
        )
    if tracks_fcast is not None and not tracks_fcast.empty:
        ax.plot(
            tracks_fcast.geometry.x,
            tracks_fcast.geometry.y,
            color=INK,
            lw=1.7,
            ls=(0, (3, 2)),
            zorder=5,
        )

    # frame on Haiti + forecast extent (don't let the full observed
    # history drag the frame across the Atlantic)
    bounds = adm0.total_bounds
    xs = [bounds[0], bounds[2]]
    ys = [bounds[1], bounds[3]]
    if tracks_fcast is not None and not tracks_fcast.empty:
        xs += [tracks_fcast.geometry.x.min(), tracks_fcast.geometry.x.max()]
        ys += [tracks_fcast.geometry.y.min(), tracks_fcast.geometry.y.max()]
    pad = 1.5
    ax.set_xlim(min(xs) - pad, max(xs) + pad)
    ax.set_ylim(min(ys) - pad, max(ys) + pad)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_color(LINE)

    track_handles = [
        Line2D([], [], color=INK, lw=1.4, label="trajectoire observée"),
        Line2D(
            [],
            [],
            color=INK,
            lw=1.4,
            ls=(0, (3, 2)),
            label="trajectoire prévue",
        ),
    ]
    leg1 = ax.legend(
        handles=track_handles,
        loc="upper left",
        frameon=True,
        framealpha=0.9,
        edgecolor=LINE,
        fontsize=8,
        labelcolor=INK,
    )
    ax.add_artist(leg1)
    if layer_handles:
        ax.legend(
            handles=layer_handles,
            title=layer_legend_title,
            loc="upper left",
            bbox_to_anchor=(1.01, 1.0),
            frameon=False,
            fontsize=7.5,
            title_fontsize=8,
            labelcolor=INK,
        )
    ax.set_title(title, fontsize=10, color=INK, loc="left", pad=8)


def storm_map_img(
    tracks_obsv,
    tracks_fcast,
    wsp_polygons,
    adm0,
    storm_name: str,
    issued_time: pd.Timestamp,
    wind_threshold_kt: int = 64,
) -> str:
    """Probabilistic map: WSP probability polygons + tracks + Haiti."""
    import geopandas as gpd

    fig, ax = _start_map()

    # WSP probability bands (percentage = lower band edge). The 0 band
    # ("<5%") would flood the whole map — draw it as a faint outline
    # only, and fill the informative bands light→dark blue.
    handles = []
    if wsp_polygons is not None and not wsp_polygons.empty:
        polys = wsp_polygons.sort_values("percentage")
        cmap = plt.cm.Blues
        for _, row in polys.iterrows():
            pct = row["percentage"]
            if pct == 0:
                gpd.GeoSeries([row["geometry"]]).plot(
                    ax=ax,
                    facecolor="none",
                    edgecolor=GREY_EDGE,
                    lw=0.6,
                    zorder=2,
                )
                continue
            frac = 0.2 + 0.75 * (pct / 90)
            color = cmap(frac)
            gpd.GeoSeries([row["geometry"]]).plot(
                ax=ax, color=color, alpha=0.72, zorder=2
            )
            handles.append(
                Patch(facecolor=color, alpha=0.72, label=f"≥ {pct} %")
            )

    _finish_map(
        fig,
        ax,
        tracks_obsv,
        tracks_fcast,
        adm0,
        f"{storm_name} — probabilités de vents "
        f"(prévision NHC du {fr_datetime(issued_time)})",
        handles,
        f"Probabilité de vents ≥ {wind_threshold_kt} kt",
    )
    return fig_to_img_tag(fig, alt=f"Carte probabiliste {storm_name}", dpi=150)


def det_map_img(
    tracks_obsv,
    tracks_fcast,
    fcast_buffers,
    obsv_buffer_geom,
    adm0,
    storm_name: str,
    issued_time: pd.Timestamp,
) -> str:
    """Deterministic map: forecast wind-radii buffers (34/50/64 kt) +
    already-observed swath + tracks + Haiti. This is the forecast the
    exposure trigger condition is computed from."""
    import geopandas as gpd

    fig, ax = _start_map()

    handles = []
    if obsv_buffer_geom is not None and not obsv_buffer_geom.is_empty:
        gpd.GeoSeries([obsv_buffer_geom]).plot(
            ax=ax, color=GREY_FILL, alpha=0.75, zorder=2
        )
        handles.append(
            Patch(
                facecolor=GREY_FILL,
                alpha=0.75,
                label="zone déjà balayée (obs.)",
            )
        )
    if fcast_buffers is not None and not fcast_buffers.empty:
        # light (34 kt) under dark (64 kt)
        for _, row in fcast_buffers.sort_values("wind_speed_kt").iterrows():
            kt = int(row["wind_speed_kt"])
            if row["geometry"] is None or row["geometry"].is_empty:
                continue
            color = WIND_COLORS.get(kt, INK_3)
            gpd.GeoSeries([row["geometry"]]).plot(
                ax=ax, color=color, alpha=0.55, zorder=3
            )
            handles.append(
                Patch(facecolor=color, alpha=0.55, label=f"vents ≥ {kt} kt")
            )

    _finish_map(
        fig,
        ax,
        tracks_obsv,
        tracks_fcast,
        adm0,
        f"{storm_name} — trajectoire et vents prévus "
        f"(prévision NHC du {fr_datetime(issued_time)})",
        handles,
        "Vents prévus (déterministe)",
    )
    return fig_to_img_tag(fig, alt=f"Carte déterministe {storm_name}", dpi=150)
