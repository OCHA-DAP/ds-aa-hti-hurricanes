"""NHC Hurricane Warnings for Haiti, 2002-2025, against the heure limite.

The Action stage can also activate on a DGPC red alert confirmed by an NHC
Hurricane Warning, held to the same 48 h cutoff as every other forecast
condition. DGPC keeps no archive of its alerts, so only the NHC half can
be replayed: which storms carried a Hurricane Warning for Haiti, and was
it issued before the cutoff?

``data/nhc_hurricane_warnings_hti.csv`` was audited in October 2026
against the watch/warning table of each storm's NHC Tropical Cyclone
Report. It lists nine storms; the hand-coded flags in
``exploration/wsp_trigger.py`` mark five (they miss Dennis 2005,
Ernesto 2006, Dean 2007 and Isaac 2012). ``advisory_time_utc`` is the
``issued_time`` of the advisory that carried the warning, as stored in
the storms DB (the synoptic time, three hours before release).
"""

from pathlib import Path

import pandas as pd

WARNINGS_CSV = Path("data/nhc_hurricane_warnings_hti.csv")


def load_warnings(path=WARNINGS_CSV):
    return pd.read_csv(
        path, parse_dates=["warning_issued_utc", "advisory_time_utc"]
    )


def with_cutoff(warnings, adv, storms):
    """Each warning with the hours to closest pass it was issued at.

    ``adv`` is ``pathways_advisories.parquet``: ``time_to_closest_h`` is
    the forecast lead of the advisory that carried the warning, and
    ``past_cutoff`` is the replay's own cutoff flag for it - the flag the
    other forecast pathways are evaluated with. ``actual_lead_h`` is the
    time from release to the storm's observed closest pass.
    """
    cols = ["atcf_id", "issued_time", "time_to_closest_h", "past_cutoff"]
    out = warnings.merge(
        adv[cols],
        left_on=["atcf_id", "advisory_time_utc"],
        right_on=["atcf_id", "issued_time"],
        how="left",
        validate="1:1",
    ).drop(columns="issued_time")
    missing = out.loc[out["time_to_closest_h"].isna(), "atcf_id"].tolist()
    if missing:
        raise RuntimeError(f"no advisory found for warnings: {missing}")
    out["pre_cutoff"] = ~out["past_cutoff"].astype(bool)
    closest = storms.set_index("atcf_id")["closest_time"]
    out["actual_lead_h"] = (
        out["atcf_id"].map(closest) - out["warning_issued_utc"]
    ).dt.total_seconds() / 3600
    return out.drop(columns="past_cutoff")
