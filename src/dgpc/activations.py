"""The framework's historical activation record, 2002-2025.

The record was assembled for the explainer deck and, until now, existed
only as hand-written rows inside ``docs/slides.html``. This module parses
those rows back into a table so the DGPC analysis can sit alongside them
without the two pages drifting apart.

The deck stays the source of truth on purpose: it is the reviewed,
published version of this record, and re-deriving it here would create a
second answer to the same question. If the deck's table changes, this
picks the change up on the next page build.

The deck's storm list is *not* the same as the DGPC storm set. It is the
storms that triggered, caused recorded impact, or drew a CERF allocation —
which includes some that passed further than D_THRESH from Haiti (Ivan
2004) and excludes near misses with no impact. Rows that have no DGPC
counterpart are kept and marked, rather than dropped.
"""

import re
from html import unescape
from pathlib import Path

import pandas as pd

SLIDES = Path("docs/slides.html")

# Column order in the deck's table.hist rows.
COLUMNS = [
    "storm",
    "fcast_exposure",
    "fcast_rain",
    "dgpc_red_hurricane_warning",
    "obsv_exposure",
    "obsv_rain",
    "triggered",
    "cerf",
    "pop_affected",
]


def _clean(cell_html: str) -> str:
    text = re.sub(r"<[^>]+>", "", cell_html)
    text = unescape(text).replace(" ", " ")
    return " ".join(text.split())


def parse_deck_activations(path: Path = SLIDES) -> pd.DataFrame:
    """Rows of the deck's `Activations historiques` table."""
    html = Path(path).read_text(encoding="utf-8")
    m = re.search(r'<table class="hist">(.*?)</table>', html, re.S)
    if not m:
        raise RuntimeError(f"no table.hist found in {path}")

    rows = []
    for tr in re.findall(r"<tr>(.*?)</tr>", m.group(1), re.S):
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)
        if len(cells) != len(COLUMNS):
            continue  # header rows use rowspan/colspan
        raw = re.findall(r"<td([^>]*)>(.*?)</td>", tr, re.S)
        rec = {c: _clean(v) for c, (_, v) in zip(COLUMNS, raw)}
        # A cell carrying class "hit" is a condition that was met.
        for col, (attrs, _) in zip(COLUMNS, raw):
            rec[f"{col}_hit"] = "hit" in attrs
        rows.append(rec)

    if not rows:
        raise RuntimeError("table.hist parsed to zero rows")

    df = pd.DataFrame(rows)
    parsed = df["storm"].str.extract(
        r"^(?P<name>.+?)\s*\((?P<season>\d{4})\)$"
    )
    df["name"] = parsed["name"].str.strip()
    df["season"] = parsed["season"].astype(int)
    df["pop_affected_n"] = pd.to_numeric(
        df["pop_affected"].str.replace(r"[^\d]", "", regex=True),
        errors="coerce",
    )
    return df


def match_to_storm_set(
    deck: pd.DataFrame, storms: pd.DataFrame
) -> pd.DataFrame:
    """Attach ``atcf_id`` by (name, season) where the storm sets overlap."""
    s = storms.copy()
    s["_name"] = s["name"].astype(str).str.strip().str.casefold()
    s["_season"] = s["season"].astype(int)

    d = deck.copy()
    d["_name"] = d["name"].str.casefold()
    d["_season"] = d["season"].astype(int)

    out = d.merge(
        s[["atcf_id", "_name", "_season"]], on=["_name", "_season"], how="left"
    )
    return out.drop(columns=["_name", "_season"])
