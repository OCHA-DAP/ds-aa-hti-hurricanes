"""Inline SVG charts for the DGPC analysis page.

Hand-built rather than matplotlib: the page is a handful of horizontal bar
charts with threshold reference lines, and writing the SVG directly keeps
the marks to spec (thin bars, rounded data-ends, a 2 px surface gap between
adjacent bars, recessive axes) at a fraction of the page weight.

Palette: categorical slots 1 and 3 for the data series (blue / aqua); the
DGPC alert levels use the reserved *status* colours, so an alert colour
never impersonates a series. Every bar is direct-labelled, which also
discharges the contrast relief owed on the aqua slot.
"""

from html import escape

SERIES = {
    "fcast": "#2a78d6",  # categorical slot 1 (blue)
    "obsv": "#1baf7a",  # categorical slot 3 (aqua)
    "extra": "#4a3aa7",  # categorical slot 7 (violet)
}
STATUS = {"orange": "#ec835a", "red": "#d03b3b"}
INK = "#1e2a2b"
MUTED = "#5e6a6b"
LINE = "#dde5e8"
SURFACE = "#ffffff"

FONT = (
    "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, "
    "'Helvetica Neue', Arial, sans-serif"
)


def _fmt(v, decimals=0):
    if v is None or v != v:
        return "—"
    s = f"{v:,.{decimals}f}".replace(",", " ")
    return s.replace(".", ",")


def grouped_barh(
    categories,
    series,
    thresholds=(),
    x_max=None,
    unit="",
    bar_h=9,
    gap=2,
    group_gap=11,
    label_w=132,
    value_w=54,
    width=760,
    x_title="",
):
    """Horizontal grouped bar chart.

    ``series`` is a list of ``(key, label, [values])``; ``thresholds`` a list
    of ``(value, label, status_key)`` drawn as labelled vertical rules.
    """
    n_series = len(series)
    row_h = n_series * bar_h + (n_series - 1) * gap + group_gap
    # Room for the legend plus two staggered rows of threshold labels.
    top_pad, bottom_pad = 62, 34
    plot_x = label_w
    plot_w = width - label_w - value_w - 10
    height = top_pad + len(categories) * row_h + bottom_pad

    all_vals = [v for _, _, vals in series for v in vals if v == v]
    hi = x_max or max(all_vals + [t[0] for t in thresholds] + [1]) * 1.06

    def sx(v):
        return plot_x + (v / hi) * plot_w

    p = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" '
        f'role="img" xmlns="http://www.w3.org/2000/svg" '
        f'style="font-family:{FONT};max-width:100%;height:auto">',
        f'<rect width="{width}" height="{height}" fill="{SURFACE}"/>',
    ]

    # Recessive x grid.
    step = _nice_step(hi)
    t = 0
    while t <= hi + 1e-9:
        x = sx(t)
        p.append(
            f'<line x1="{x:.1f}" y1="{top_pad - 8}" x2="{x:.1f}" '
            f'y2="{height - bottom_pad}" stroke="{LINE}" stroke-width="1"/>'
        )
        p.append(
            f'<text x="{x:.1f}" y="{height - bottom_pad + 15}" fill="{MUTED}" '
            f'font-size="10" text-anchor="middle">{_fmt(t)}</text>'
        )
        t += step

    if x_title:
        p.append(
            f'<text x="{plot_x + plot_w / 2:.1f}" y="{height - 6}" '
            f'fill="{MUTED}" font-size="10.5" text-anchor="middle">'
            f"{escape(x_title)}</text>"
        )

    # Threshold rules, drawn under the bars. Labels stagger across two rows
    # so adjacent thresholds never overprint each other.
    for i, (val, lab, key) in enumerate(thresholds):
        if val > hi:
            continue
        x = sx(val)
        col = STATUS[key]
        y_lab = top_pad - 33 + (i % 2) * 14
        p.append(
            f'<line x1="{x:.1f}" y1="{y_lab + 3}" x2="{x:.1f}" '
            f'y2="{height - bottom_pad}" stroke="{col}" stroke-width="1.5" '
            f'stroke-dasharray="4 3"/>'
        )
        # Keep the label inside the plot: flip to the left near the edge.
        anchor = "end" if x > plot_x + plot_w * 0.62 else "start"
        dx = -6 if anchor == "end" else 6
        p.append(
            f'<text x="{x + dx:.1f}" y="{y_lab}" fill="{col}" '
            f'font-size="10.5" font-weight="700" text-anchor="{anchor}">'
            f"{escape(lab)}</text>"
        )

    # Bars.
    for r, cat in enumerate(categories):
        y0 = top_pad + r * row_h
        cy = y0 + (n_series * bar_h + (n_series - 1) * gap) / 2
        p.append(
            f'<text x="{label_w - 10}" y="{cy + 3.5:.1f}" fill="{INK}" '
            f'font-size="11.5" text-anchor="end">{escape(str(cat))}</text>'
        )
        for si, (key, _lab, vals) in enumerate(series):
            v = vals[r]
            y = y0 + si * (bar_h + gap)
            if v != v or v is None:
                p.append(
                    f'<text x="{plot_x + 4}" y="{y + bar_h - 1:.1f}" '
                    f'fill="{MUTED}" font-size="10">—</text>'
                )
                continue
            w = max(sx(v) - plot_x, 1.5)
            p.append(
                f'<rect x="{plot_x}" y="{y:.1f}" width="{w:.1f}" '
                f'height="{bar_h}" rx="3.5" fill="{SERIES[key]}"/>'
            )
            # Halo so a value label stays legible where it crosses a
            # threshold rule.
            p.append(
                f'<text x="{plot_x + w + 6:.1f}" y="{y + bar_h - 0.8:.1f}" '
                f'fill="{MUTED}" font-size="9.8" paint-order="stroke" '
                f'stroke="{SURFACE}" stroke-width="3" '
                f'stroke-linejoin="round">{_fmt(v)}{unit}</text>'
            )

    # Axis spine.
    p.append(
        f'<line x1="{plot_x}" y1="{top_pad - 8}" x2="{plot_x}" '
        f'y2="{height - bottom_pad}" stroke="{MUTED}" stroke-width="1"/>'
    )

    # Legend.
    lx = plot_x
    for key, lab, _ in series:
        p.append(
            f'<rect x="{lx}" y="6" width="19" height="8" rx="3" '
            f'fill="{SERIES[key]}"/>'
        )
        p.append(
            f'<text x="{lx + 25}" y="13.5" fill="{INK}" font-size="11">'
            f"{escape(lab)}</text>"
        )
        lx += 32 + len(lab) * 6.1
    p.append("</svg>")
    return "".join(p)


def _nice_step(hi):
    """Smallest 1/2/2.5/5 x 10^n step giving at most 8 gridlines."""
    candidates = sorted(
        mult * 10**exp for exp in range(-2, 7) for mult in (1, 2, 2.5, 5)
    )
    for step in candidates:
        if hi / step <= 8:
            return step
    return hi / 5
