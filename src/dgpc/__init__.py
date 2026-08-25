"""DGPC alert-level criteria: historical assessment.

The DGPC (Direction Générale de la Protection Civile) alert levels as
communicated to OCHA in August 2026:

- **Orange**: 100 mm/24 h rainfall OR 100-120 km/h wind
- **Red**: 60-80 mm/h rainfall OR 300 mm/6-12 h rainfall OR >=200 km/h wind

DGPC did not specify whether these are forecast or observed values, nor
which data sources or spatial aggregation they refer to. The assumptions
this analysis makes are documented in ``src/dgpc/constants.py`` and on the
published page (``docs/dgpc-alertes.html``).
"""
