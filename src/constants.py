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
