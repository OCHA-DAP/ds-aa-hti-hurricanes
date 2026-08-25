"""DGPC alert-level thresholds and the assumptions used to evaluate them."""

# --- Wind ---------------------------------------------------------------
# DGPC gave wind in km/h. NHC works in knots, so both are kept explicit.
KMH_TO_KT = 1 / 1.852

ORANGE_WIND_KMH = (100, 120)  # "100-120 km/h"
RED_WIND_KMH = 200  # ">=200 km/h"

ORANGE_WIND_KT = ORANGE_WIND_KMH[0] * KMH_TO_KT  # 54.0 kt
ORANGE_WIND_UPPER_KT = ORANGE_WIND_KMH[1] * KMH_TO_KT  # 64.8 kt
RED_WIND_KT = RED_WIND_KMH * KMH_TO_KT  # 108.0 kt

# The orange band is read as a *lower bound* (>=100 km/h), not a closed
# band: a storm bringing 150 km/h to Haiti has self-evidently met the
# orange wind condition as well as being on the way to red. The upper
# bound is reported separately so the reading can be revisited with DGPC.

# NHC wind radii describe the maximum 1-minute sustained 10 m wind over
# *water*. Surface friction reduces that over land, and DGPC's levels are
# explicitly about wind on land, so a reduction factor is applied. 0.85 is
# the conventional open-terrain value (Vickery et al.; ASCE 7 commentary).
# DGPC did not say whether their figures are sustained winds or gusts, and
# public warnings often quote gusts, so all three readings are reported.
LAND_REDUCTION_FACTOR = 0.85
GUST_FACTOR = 1.25  # 3-second gust / 1-minute sustained, over land

WIND_VARIANTS = {
    "sustained_land": "Sustained wind over land (x0.85)",
    "sustained_marine": "Sustained wind, unreduced (over-water equivalent)",
    "gust_land": "Gust over land (x0.85 x1.25)",
}
PRIMARY_WIND_VARIANT = "sustained_land"

# --- Rainfall -----------------------------------------------------------
# (window_hours, threshold_mm). The red rate criterion "60-80 mm/h" is
# taken at its lower bound, 60 mm in any 1 h.
ORANGE_RAIN = {"window_h": 24, "threshold_mm": 100}
RED_RAIN_RATE = {"window_h": 1, "threshold_mm": 60}
RED_RAIN_RATE_UPPER_MM = 80
# "300 mm/(6-12 hrs)" is read as 300 mm accumulating in any 12 h window
# (the more permissive, and more physically attainable, end of the range);
# the 6 h version is reported alongside it.
RED_RAIN_ACCUM = {"window_h": 12, "threshold_mm": 300}
RED_RAIN_ACCUM_STRICT = {"window_h": 6, "threshold_mm": 300}

# --- Spatial aggregations -----------------------------------------------
# DGPC did not specify the area over which a threshold must be met.
# All three are computed and reported side by side.
AGGREGATIONS = ("national_mean", "department_max", "any_pixel")

# --- Analysis window ----------------------------------------------------
# NHC began forecasting 64-kt wind radii in 2002; IMERG starts 2000-06.
# 2002-2025 is the common window, and matches the framework's own
# historical activation record.
SEASON_START = 2002
SEASON_END = 2025

# Storms are attributed to Haiti using the framework's existing distance
# gate (src.constants.D_THRESH = 230 km from the adm0 boundary).

# The 2026 framework’s overall return period, for comparison.
FRAMEWORK_RP_YEARS = 2.4
