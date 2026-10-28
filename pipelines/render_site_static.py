"""Write the monitoring status into the Pages site as plain text.

The monitoring page draws everything with JavaScript, so a reader that
does not run it (the web tools of most AI assistants, curl, crawlers)
gets no status from it. Run by .github/workflows/deploy-pages.yml right
after sync_site_data.py, this fills the block between

    <!-- status:start -->
    <!-- status:end -->

in the two monitoring pages (French, English) and the two llms.txt files
from the index.json just downloaded, so the text always matches the
published data. Works on the assembled site directory, never on docs/;
standard library only.

    python pipelines/render_site_static.py SITE_DIR

Exits non-zero if index.json or a marker is missing.
"""

import html
import json
import re
import sys
from datetime import datetime
from pathlib import Path

BASE = "https://ocha-dap.github.io/ds-aa-hti-hurricanes/2026/suivi/"
MARKERS = re.compile(
    r"(<!-- status:start -->\n).*?(<!-- status:end -->)", re.S
)
STAGES = ("mobilisation", "action", "obsv")

# file -> (format, language, path from the file to the monitoring page)
TARGETS = {
    "2026/suivi/index.html": ("html", "fr", ""),
    "2026/suivi/en/index.html": ("html", "en", "../"),
    "2026/suivi/llms.txt": ("md", "en", BASE),
    "llms.txt": ("md", "en", BASE),
}

MONTHS = {
    "en": "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(),
    "fr": "janv. févr. mars avr. mai juin juil. août sept. oct. nov. déc.".split(),
}

# Same wording as the stage cards in docs/2026/suivi/app.js.
TEXT = {
    "en": {
        "title": "Current status",
        "asof": lambda t: f"Data as of {t}. The page and its files are "
        "refreshed after each run of the monitoring system, about every "
        "6 hours.",
        "stage": {
            "mobilisation": "Mobilisation",
            "action": "Action",
            "obsv": "Early response (Réponse précoce)",
        },
        "desc": {
            "mobilisation": lambda mm, kt, h: f"≥ {mm} mm forecast over "
            f"2 days OR > 0 people exposed to {kt} kt winds, forecasts up "
            f"to {h} h",
            "action": lambda mm, kt, h: f"same conditions, forecasts up to "
            f"{h} h; its two DGPC-alert conditions are not monitored here",
            "obsv": lambda mm, kt, h: f"≥ {mm} mm observed over 2 days OR "
            f"> 0 people exposed to observed {kt} kt winds",
        },
        "not_reached": "not reached this season.",
        "reached": "reached this season: ",
        "met": "conditions met this season, but no separate activation: ",
        "hit": lambda n, a, t: f"{n} ({a}), first triggered {t}",
        "hit_met": lambda n, a, t: f"{n} ({a}), conditions first met {t}",
        "no_sep": " (Action had already been triggered for this storm)",
        "sep": "; ",
        "active": "Active storms",
        "none": "none.",
        "adv": lambda n, a, t, km, mob, act: f"{n} ({a}): latest advisory "
        f"{t}, closest approach to Haiti in it {km} km; Mobilisation "
        f"triggered by it: {mob}; Action: {act}",
        "yes": "yes",
        "no": "no",
        "latest": "Latest data",
        "sources": lambda adv, gefs, imerg: f"NHC advisory {adv}; "
        f"CHIRPS-GEFS rainfall forecast issued {gefs}; IMERG observed "
        f"rainfall through {imerg}.",
        "links": lambda p: f'Machine-readable: <a href="{p}data/index.json">'
        f"data/index.json</a> (current state) · "
        f'<a href="{p}data/datapackage.json">data/datapackage.json</a> '
        f"(every file and field) · "
        f'<a href="{p}llms.txt">llms.txt</a> (guide for scripts and AI '
        f"assistants).",
    },
    "fr": {
        "title": "Situation actuelle",
        "asof": lambda t: f"Données au {t}. La page et ses fichiers sont "
        "mis à jour après chaque passage du système de suivi, environ "
        "toutes les 6 heures.",
        "stage": {},  # name_fr from index.json
        "desc": {
            "mobilisation": lambda mm, kt, h: f"≥ {mm} mm prévus sur "
            f"2 jours OU > 0 personne exposée aux vents de {kt} kt, "
            f"prévisions jusqu’à {h} h",
            "action": lambda mm, kt, h: f"mêmes conditions, prévisions "
            f"jusqu’à {h} h ; ses deux conditions fondées sur les alertes "
            "de la DGPC ne sont pas suivies ici",
            "obsv": lambda mm, kt, h: f"≥ {mm} mm observés sur 2 jours OU "
            f"> 0 personne exposée aux vents de {kt} kt observés",
        },
        "not_reached": "non atteint cette saison.",
        "reached": "atteint cette saison : ",
        "met": "conditions remplies cette saison, mais pas d’activation "
        "séparée : ",
        "hit": lambda n, a, t: f"{n} ({a}), premier déclenchement le {t}",
        "hit_met": lambda n, a, t: f"{n} ({a}), conditions remplies pour la "
        f"première fois le {t}",
        "no_sep": " (l’Action avait déjà été déclenchée pour cette tempête)",
        "sep": " ; ",
        "active": "Tempêtes actives",
        "none": "aucune.",
        "adv": lambda n, a, t, km, mob, act: f"{n} ({a}) : dernier avis "
        f"le {t}, passage le plus proche d’Haïti dans cet avis {km} km ; "
        f"Mobilisation déclenchée par cet avis : {mob} ; Action : {act}",
        "yes": "oui",
        "no": "non",
        "latest": "Dernières données",
        "sources": lambda adv, gefs, imerg: f"avis du NHC du {adv} ; "
        f"prévision de pluie CHIRPS-GEFS émise le {gefs} ; pluie observée "
        f"IMERG jusqu’au {imerg}.",
        "links": lambda p: f'Lisible par machine : <a href="{p}data/index.json">'
        f"data/index.json</a> (état actuel) · "
        f'<a href="{p}data/datapackage.json">data/datapackage.json</a> '
        f"(chaque fichier et champ) · "
        f'<a href="{p}llms.txt">llms.txt</a> (guide pour les scripts et '
        f"les assistants IA, en anglais).",
    },
}


def fmt_time(iso: str | None, lang: str) -> str:
    if not iso:
        return "—"
    d = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return f"{d.day} {MONTHS[lang][d.month - 1]} {d.year}, {d:%H:%M} UTC"


def fmt_date(day: str | None, lang: str) -> str:
    if not day:
        return "—"
    d = datetime.strptime(day, "%Y-%m-%d")
    return f"{d.day} {MONTHS[lang][d.month - 1]} {d.year}"


def fmt_km(km: float | None, lang: str) -> str:
    if km is None:
        return "—"
    s = f"{round(km):,}"
    return s.replace(",", " ") if lang == "fr" else s


def status(idx: dict, lang: str) -> tuple[str, list[tuple]]:
    """The status as (intro, [(label, condition or None, text), ...])."""
    t = TEXT[lang]
    trig, kt = idx["triggers"], idx["exposure_wind_kt"]
    items = []
    for k in STAGES:
        name = t["stage"].get(k) or trig[k]["name_fr"]
        desc = t["desc"][k](trig[k]["rain_mm"], kt, trig[k].get("lt_max_hrs"))
        season = idx["season_activations"].get(k)
        storms = sorted(
            (s for s in idx["storms"] if s["activations"].get(k)),
            key=lambda s: s["activations"][k]["issue_time"],
        )
        if not season:
            items.append((name, desc, t["not_reached"]))
            continue
        hits = []
        for s in storms:
            a = s["activations"][k]
            sep = a.get("separate_activation") is not False
            hit = t["hit" if sep else "hit_met"](
                s["name"], s["atcf_id"], fmt_time(a["issue_time"], lang)
            )
            hits.append(hit if sep else hit + t["no_sep"])
        head = t[
            "met" if season.get("separate_activation") is False else "reached"
        ]
        items.append((name, desc, head + t["sep"].join(hits) + "."))

    active = [
        t["adv"](
            s["name"],
            s["atcf_id"],
            fmt_time(s["latest"]["issue_time"], lang),
            fmt_km(s["latest"].get("min_dist_km"), lang),
            t["yes" if s["latest"].get("mobilisation_trigger") else "no"],
            t["yes" if s["latest"].get("action_trigger") else "no"],
        )
        for s in idx["storms"]
        if s.get("active") and s.get("latest")
    ]
    items.append(
        (
            t["active"],
            None,
            t["sep"].join(active) + "." if active else t["none"],
        )
    )

    src = idx["sources"]
    last = src.get("last_advisory_issue_time")
    by = next((s for s in idx["storms"] if s["last_issue_time"] == last), None)
    adv = fmt_time(last, lang) + (f" ({by['name']})" if by else "")
    items.append(
        (
            t["latest"],
            None,
            t["sources"](
                adv,
                fmt_date(src.get("chirps_gefs_last_issue_date"), lang),
                fmt_date(src.get("imerg_last_date"), lang),
            ),
        )
    )
    return t["asof"](fmt_time(idx["generated_at"], lang)), items


def as_html(idx: dict, lang: str, prefix: str) -> str:
    intro, items = status(idx, lang)
    e = html.escape
    colon = " :" if lang == "fr" else ":"
    lis = "\n".join(
        f"  <li><b>{e(label)}</b>{f' ({e(cond)})' if cond else ''}{colon} "
        f"{e(text)}</li>"
        for label, cond, text in items
    )
    return (
        '<section id="summary" class="summary">\n'
        f"<h2>{TEXT[lang]['title']}</h2>\n"
        f"<p>{e(intro)}</p>\n<ul>\n{lis}\n</ul>\n"
        f'<p class="small muted">{TEXT[lang]["links"](prefix)}</p>\n'
        "</section>\n"
    )


def as_markdown(idx: dict, lang: str, base: str) -> str:
    intro, items = status(idx, lang)
    lis = "\n".join(
        f"- **{label}**{f' ({cond})' if cond else ''}: {text}"
        for label, cond, text in items
    )
    return (
        f"{intro}\n\n{lis}\n\n"
        f"Current values, as JSON: <{base}data/index.json>\n"
    )


def main(site: str) -> int:
    root = Path(site)
    idx = json.loads((root / "2026/suivi/data/index.json").read_text())
    for rel, (fmt, lang, prefix) in TARGETS.items():
        path = root / rel
        text = path.read_text()
        body = (as_html if fmt == "html" else as_markdown)(idx, lang, prefix)
        new, n = MARKERS.subn(
            lambda m: m.group(1) + body + m.group(2), text, count=1
        )
        if n != 1:
            print(f"{rel}: status markers not found", file=sys.stderr)
            return 1
        path.write_text(new)
        print(f"Status written to {rel}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
