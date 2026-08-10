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

_EMAIL_CONTENT_WIDTH_PX = 900

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


def _pdf_atoms(
    bands: list[tuple[int, int]], floor: float, upper: float
) -> list[tuple[float, float]]:
    """Discrete pmf of total exposure under the comonotone
    one-severity-draw model (ds-storms-alerts WspPdf).

    One severity draw U~Uniform(0,1); a location is exposed iff its WSP
    probability >= U. So exposure equals the cumulative band population
    C_k with probability p_k - p_{k+1} (band midpoints descending), and
    stays at the already-observed floor with probability 1 - p_max.
    Values are clamped at the country population; clamped atoms merge.
    """
    probs = sorted(
        (
            (WSP_BAND_MIDPOINT.get(int(p), 0.025), int(pop))
            for p, pop in bands
            if pop > 0
        ),
        reverse=True,
    )
    p_list = [p for p, _ in probs] + [0.0]
    atoms = [(min(float(floor), upper), 1.0 - (p_list[0] if probs else 0.0))]
    cum = float(floor)
    for i, (_, pop) in enumerate(probs):
        cum += pop
        atoms.append((min(cum, upper), p_list[i] - p_list[i + 1]))
    # merge atoms clamped onto the same value
    merged: dict[float, float] = {}
    for v, w in atoms:
        merged[v] = merged.get(v, 0.0) + w
    return sorted(merged.items())


def _kernel_density(
    atoms: list[tuple[float, float]],
    upper: float,
    x_win: float,
    n: int = 600,
):
    """Gaussian-kernel smoothing of the pmf atoms, reflected at 0 and at
    the population cap so mass piles at the bounds instead of leaking.
    Grid and bandwidth scale to the display window x_win."""
    xs = np.linspace(0, x_win, n)
    h = max(x_win * 0.025, 1.0)
    dens = np.zeros_like(xs)

    def _phi(z):
        return np.exp(-0.5 * z**2)

    for v, w in atoms:
        if w <= 0:
            continue
        dens += w * (
            _phi((xs - v) / h)
            + _phi((xs + v) / h)
            + _phi((2 * upper - xs - v) / h)
        )
    return xs, dens


def wsp_density_img(
    df_wsp: pd.DataFrame,
    storm_name: str,
    issued_time: pd.Timestamp,
    obsv_floor_by_kt: dict | None = None,
    total_pop: float | None = None,
) -> str:
    """Stacked panels (34/50/64 kt): smoothed probability density of the
    population exposed, from the NHC WSP bands.

    df_wsp: [wind_threshold_kt, percentage, pop_exposed] for one issuance
    (see storms_db.fetch_wsp_exposure).
    """
    obsv_floor_by_kt = obsv_floor_by_kt or {}
    total_pop = float(total_pop or 11_757_597)

    levels = (34, 50, 64)
    curves = {}
    x_max = 0.0
    for kt in levels:
        sub = df_wsp[df_wsp["wind_threshold_kt"] == kt]
        floor = float(obsv_floor_by_kt.get(kt, 0))
        bands = list(zip(sub["percentage"], sub["pop_exposed"]))
        atoms = _pdf_atoms(bands, floor, total_pop)
        x_max = max(x_max, max(v for v, _ in atoms))
        curves[kt] = atoms
    x_max = min(max(x_max * 1.08, 1000.0), total_pop)

    fig, axes = plt.subplots(
        len(levels),
        1,
        figsize=(9.0, 4.6),
        dpi=200,
        sharex=True,
        gridspec_kw={"hspace": 0.35},
    )
    for ax, kt in zip(axes, levels):
        color = WIND_COLORS[kt]
        xs, dens = _kernel_density(curves[kt], total_pop, x_max)
        peak = dens.max()
        if peak > 0:
            dens = dens / peak
        ax.fill_between(xs, 0, dens, color=color, alpha=0.35, zorder=2)
        ax.plot(xs, dens, color=color, lw=1.8, zorder=3)
        ax.set_xlim(0, x_max)
        ax.set_ylim(0, 1.12)
        ax.set_yticks([])
        for spine in ("top", "right", "left"):
            ax.spines[spine].set_visible(False)
        ax.spines["bottom"].set_color(LINE)
        ax.spines["bottom"].set_linewidth(1.2)
        ax.text(
            0.995,
            0.86,
            f"vents ≥ {kt} kt",
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=10,
            fontweight="bold",
            color=color,
            bbox={
                "facecolor": "white",
                "alpha": 0.75,
                "edgecolor": "none",
                "pad": 2,
            },
        )
        if ax is axes[-1]:
            ax.xaxis.set_major_formatter(lambda x, _: fmt_pop(x))
            ax.tick_params(colors=INK_2, labelsize=9)
            ax.set_xlabel("Population exposée", color=INK_2, fontsize=10)
        else:
            ax.tick_params(bottom=False, labelbottom=False)

    axes[0].set_title(
        "Distribution de probabilité de la population exposée\n"
        f"(NHC Wind Speed Probabilities, prévision du "
        f"{fr_datetime(issued_time)})",
        fontsize=11,
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
    fig, ax = plt.subplots(figsize=(9.0, 6.2), dpi=200)
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
        fontsize=9,
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
            fontsize=8.5,
            title_fontsize=9,
            labelcolor=INK,
        )
    ax.set_title(title, fontsize=11.5, color=INK, loc="left", pad=8)


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
    return fig_to_img_tag(fig, alt=f"Carte probabiliste {storm_name}", dpi=200)


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
    return fig_to_img_tag(fig, alt=f"Carte déterministe {storm_name}", dpi=200)
