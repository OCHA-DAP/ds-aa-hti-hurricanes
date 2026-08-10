import numpy as np

HTI_ASAP0_ID = 68
LAURA_ATCF_ID = "al132020"
MATTHEW_ATCF_ID = "al142016"
IVAN_ATCF_ID = "al092004"
SANDY_ATCF_ID = "al182012"
JEANNE_ATCF_ID = "al112004"
HANNA_ATCF_ID = "al082008"
GUSTAV_ATCF_ID = "al072008"
IKE_ATCF_ID = "al092008"

# v1 (2024) trigger distance gate; kept for historical analysis only.
# Also still used to attribute rainfall date-windows to a storm (the same
# attribution used when calibrating the rainfall thresholds).
D_THRESH = 230

CERF_SIDS = [
    "2016273N13300",  # Matthew
    "2008245N17323",  # Ike
    "2008238N13293",  # Gustav
    "2008241N19303",  # Hanna
    "2008229N18293",  # Fay
    "2012296N14283",  # Sandy
]

FRENCH_MONTHS = {
    "Jan": "jan.",
    "Feb": "fév.",
    "Mar": "mars",
    "Apr": "avr.",
    "May": "mai",
    "Jun": "juin",
    "Jul": "juil.",
    "Aug": "août",
    "Sep": "sept.",
    "Oct": "oct.",
    "Nov": "nov.",
    "Dec": "déc.",
}

CHD_GREEN = "#1bb580"

LON_ZOOM_RANGE = np.array(
    [
        0.0007,
        0.0014,
        0.003,
        0.006,
        0.012,
        0.024,
        0.048,
        0.096,
        0.192,
        0.3712,
        0.768,
        1.536,
        3.072,
        6.144,
        11.8784,
        23.7568,
        47.5136,
        98.304,
        190.0544,
        360.0,
    ]
)

# No trigger may fire once the storm is forecast to make landfall or pass
# closest to Haiti within this many hours (informational emails still go out,
# flagged as past-cutoff).
LT_CUTOFF_HRS = 48

# Wind level whose population exposure drives the exposure conditions.
EXPOSURE_WIND_KT = 64

# 2026 framework trigger definition. Each forecast stage fires (pre-cutoff
# issuances only) on:
#   forecast 2-day rolling rainfall >= rain_mm
#   OR forecast population exposed to >= EXPOSURE_WIND_KT winds > 0
#   (fcastonly exposure capped at lt_max_hrs leadtime + cumulative observed)
# The observational stage fires on:
#   observed 2-day rolling rainfall >= rain_mm
#   OR observed population exposed to >= EXPOSURE_WIND_KT winds > 0
# The Action stage's third condition (DGPC red alert confirmed by an NHC
# Hurricane Warning) is not yet implemented in this monitoring system.
TRIGGERS = {
    "mobilisation": {"rain_mm": 68, "lt_max_hrs": 120, "rp_years": 3.0},
    "action": {"rain_mm": 68, "lt_max_hrs": 72, "rp_years": 3.0},
    "obsv": {"rain_mm": 57, "lt_max_hrs": None, "rp_years": 3.4},
}
OVERALL_RP_YEARS = 2.4

# French display names for the trigger stages.
STAGE_NAMES_FR = {
    "mobilisation": "Mobilisation",
    "action": "Action",
    "obsv": "Réponse précoce",
}

# Listmonk lists (see pipelines/setup_listmonk_lists.py).
LISTMONK_PROJECT_TAG = "ds-aa-hti-hurricanes"
LISTMONK_INFO_LIST_ID = 116  # AA Haïti ouragans - informations
LISTMONK_TRIGGER_LIST_ID = 117  # AA Haïti ouragans - déclencheurs
# "[TEST] Storm Alerts - Internal Test" (private; DS team members).
LISTMONK_TEST_LIST_ID = 110

MIN_EMAIL_DISTANCE = 1000

NUMERIC_NAME_REGEX = r"\b(?:One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|Eleven|Twelve|Thirteen|Fourteen|Fifteen|Sixteen|Seventeen|Eighteen|Nineteen|Twenty)\b"  # noqa: E501
