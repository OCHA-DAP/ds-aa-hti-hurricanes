"""Parametric tropical-cyclone wind field over Haiti, fitted to NHC
forecast quadrant wind radii.

Why a parametric field at all
-----------------------------
NHC forecasts carry quadrant radii for 34, 50 and 64 kt only. The DGPC
wind levels are 100 km/h (54 kt), 120 km/h (65 kt) and 200 km/h (108 kt) —
none of which is a forecast radius. 54 and 65 kt sit *between* provided
radii, so they interpolate; 108 kt lies inside the 64-kt radius, so it is
an extrapolation toward the core and is only meaningful when the forecast
Vmax already exceeds 108 kt.

The profile
-----------
Per bearing, wind is a piecewise power law through the anchor points

    (Rmw, Vmax), (r64, 64), (r50, 50), (r34, 34)

linear in log V vs log r, so it reproduces every NHC-provided radius
exactly and interpolates smoothly between them. Inside Rmw wind falls
linearly to zero at the centre. Rmw is not forecast by NHC (the a-deck
OFCL field is always 0), so it comes from the Willoughby et al. (2006)
climatological relation

    Rmw [km] = 46.4 * exp(-0.0155 * Vmax [m/s] + 0.0169 * |lat|)

This is the analysis's main structural uncertainty and it bears almost
entirely on the 108-kt (red) level. The ``rmw_scale`` argument threaded
through ``wind_at_points``/``max_wind_field`` exists to quantify it:
``pipelines/run_dgpc_wind.py`` re-runs the observed track at x0.79 and
x1.40, the observed inter-quartile range of Rmw around the climatological
value at >=108 kt.

Quadrant radii are interpolated smoothly across bearing (values are taken
to apply at the quadrant bisectors 45/135/225/315 degrees) rather than
stepping at quadrant boundaries.
"""

import ast

import numpy as np
import pandas as pd

KT_TO_MS = 0.514444
NM_TO_KM = 1.852
RAD_LEVELS = (34, 50, 64)
# Quadrant order in the storms DB is [NE, SE, SW, NW]; bisector bearings.
QUAD_BEARINGS = np.array([45.0, 135.0, 225.0, 315.0])
EARTH_R_KM = 6371.0


def parse_quad(v) -> np.ndarray:
    """Quadrant radii (nm) as a length-4 array; missing -> NaN.

    Handles both storage forms in the storms DB: the NHC tables hold a
    JSON-ish list ``'[40, 0, 0, 30]'``, IBTrACS holds Postgres array text
    ``'{40,0,0,30}'`` (which ``literal_eval`` would turn into a *set* and
    silently scramble the quadrant order).
    """
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return np.full(4, np.nan)
    if isinstance(v, str):
        s = v.strip()
        if s.startswith("{") and s.endswith("}"):
            parts = [p.strip() for p in s[1:-1].split(",") if p.strip()]
            v = [
                np.nan if p.upper() in ("NULL", "NAN", "") else float(p)
                for p in parts
            ]
        else:
            v = ast.literal_eval(s)
    if v is None:
        return np.full(4, np.nan)
    arr = np.asarray(list(v), dtype=float)
    if arr.size != 4:
        return np.full(4, np.nan)
    return arr


def rmw_climo_km(vmax_kt, lat_deg):
    """Willoughby et al. (2006) radius of maximum wind, km."""
    vmax_ms = np.asarray(vmax_kt, dtype=float) * KT_TO_MS
    return 46.4 * np.exp(-0.0155 * vmax_ms + 0.0169 * np.abs(lat_deg))


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance, km. Broadcasts."""
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp = p2 - p1
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * EARTH_R_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def bearing_deg(lat1, lon1, lat2, lon2):
    """Initial bearing from point 1 to point 2, degrees clockwise from N."""
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dl = np.radians(lon2 - lon1)
    y = np.sin(dl) * np.cos(p2)
    x = np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dl)
    return np.degrees(np.arctan2(y, x)) % 360


def interp_radii_by_bearing(quad_radii_km, bearings):
    """Interpolate a quadrant radius set across bearing.

    ``quad_radii_km`` is length 4 ([NE, SE, SW, NW]) at bearings
    45/135/225/315. Returns radii at each requested bearing, wrapping
    circularly. NaN quadrants propagate as NaN.
    """
    b = np.asarray(bearings, dtype=float) % 360
    # Extend the cycle so interpolation wraps: 315-360 -> back to 45.
    xp = np.concatenate(
        [QUAD_BEARINGS - 360, QUAD_BEARINGS, QUAD_BEARINGS + 360]
    )
    fp = np.concatenate([quad_radii_km, quad_radii_km, quad_radii_km])
    return np.interp(b, xp, fp)


def _anchors_for_point(vmax_kt, lat, quad_radii_nm, bearings):
    """Build (radii, winds) anchor arrays per bearing for one track point.

    ``quad_radii_nm`` is a (3, 4) array of [34, 50, 64] x [NE, SE, SW, NW]
    radii in nautical miles. Returns ``(R, V)`` each shaped (4, n_bearings),
    ordered from the core outward: [Rmw, r64, r50, r34].
    """
    rmw = rmw_climo_km(vmax_kt, lat)
    n = np.size(bearings)
    R = np.empty((4, n))
    V = np.empty((4, n))
    R[0] = rmw
    V[0] = vmax_kt

    for i, level in enumerate(reversed(RAD_LEVELS)):  # 64, 50, 34
        row = quad_radii_nm[len(RAD_LEVELS) - 1 - i]
        V[i + 1] = level
        if level >= vmax_kt or np.all(np.isnan(row)):
            # This wind level does not exist in this storm (level above
            # Vmax), or its radius was not forecast. Collapse the anchor
            # onto the core anchor entirely - radius AND wind - so the
            # next segment interpolates down from Vmax. Leaving the wind
            # at `level` here would make the profile descend from a speed
            # the storm never reached.
            R[i + 1] = rmw
            V[i + 1] = vmax_kt
            continue
        # A quadrant radius of 0 means "no winds this strong in this
        # quadrant" -> the level reaches only the eyewall there.
        row = np.where(np.isnan(row), 0.0, row) * NM_TO_KM
        row = np.where(row <= 0, rmw, row)
        R[i + 1] = interp_radii_by_bearing(row, bearings)

    # Enforce monotonicity outward (r must grow as wind level falls).
    for i in range(1, 4):
        R[i] = np.maximum(R[i], R[i - 1])
    return R, V


def wind_at_points(
    vmax_kt, lat, lon, quad_radii_nm, grid_lat, grid_lon, rmw_scale=1.0
):
    """Wind speed (kt) at each grid point for a single track point.

    ``rmw_scale`` multiplies the climatological Rmw — used for the
    sensitivity test on the red (108 kt) level.
    """
    if not np.isfinite(vmax_kt) or vmax_kt <= 0:
        return np.zeros(np.shape(grid_lat))

    r = haversine_km(lat, lon, grid_lat, grid_lon)
    b = bearing_deg(lat, lon, grid_lat, grid_lon)
    R, V = _anchors_for_point(vmax_kt, lat, quad_radii_nm, b.ravel())
    R = R * rmw_scale
    R = R.reshape((4,) + np.shape(r))
    V = V.reshape((4,) + np.shape(r))

    out = np.zeros(np.shape(r))
    rr = np.maximum(r, 1e-6)

    # Inside the eyewall: linear rise to Vmax.
    inner = rr < R[0]
    out = np.where(inner, V[0] * rr / np.maximum(R[0], 1e-6), out)

    # Piecewise log-log segments between consecutive anchors.
    for i in range(3):
        r_in, r_out = R[i], R[i + 1]
        v_in, v_out = V[i], V[i + 1]
        seg = (rr >= r_in) & (rr < r_out) & (r_out > r_in)
        if not np.any(seg):
            continue
        with np.errstate(divide="ignore", invalid="ignore"):
            # The denominator vanishes where r_out == r_in (a collapsed
            # anchor); `seg` already excludes those cells.
            frac = np.where(
                seg,
                (np.log(rr) - np.log(np.maximum(r_in, 1e-6)))
                / np.log(np.maximum(r_out, 1e-6) / np.maximum(r_in, 1e-6)),
                0.0,
            )
        out = np.where(
            seg,
            np.exp(np.log(v_in) + frac * (np.log(v_out) - np.log(v_in))),
            out,
        )

    # Beyond the outermost anchor, continue the last segment's decay.
    outer = rr >= R[3]
    if np.any(outer):
        with np.errstate(divide="ignore", invalid="ignore"):
            x = np.where(
                R[3] > R[2],
                np.log(V[2] / V[3]) / np.log(R[3] / np.maximum(R[2], 1e-6)),
                0.5,
            )
        x = np.where(np.isfinite(x), x, 0.5)
        x = np.clip(x, 0.2, 1.5)
        out = np.where(outer, V[3] * (R[3] / rr) ** x, out)

    return out


def selfcheck(verbose=True):
    """Assert the profile's invariants. Run: ``python -m src.dgpc.windfield``.

    Guards three properties that can break silently:
    1. every NHC-published radius is reproduced at its stated wind level;
    2. wind never exceeds Vmax anywhere (a collapsed anchor must not leave
       a phantom wind level above the storm's own intensity);
    3. wind decreases monotonically outside the radius of maximum wind.
    """
    lat, lon = 18.5, -72.5

    def probe(vmax, radii, r_km, brg=45.0):
        dlat = r_km / 111.32 * np.cos(np.radians(brg))
        dlon = (
            r_km / (111.32 * np.cos(np.radians(lat))) * np.sin(np.radians(brg))
        )
        return wind_at_points(
            vmax,
            lat,
            lon,
            radii,
            np.array([[lat + dlat]]),
            np.array([[lon + dlon]]),
        ).item()

    cases = {
        "major hurricane, all radii": (
            120.0,
            np.array(
                [[160, 110, 90, 120], [80, 60, 50, 50], [35, 35, 30, 30]],
                dtype=float,
            ),
        ),
        "tropical storm, only r34": (
            45.0,
            np.array(
                [[100, 100, 100, 100], [0, 0, 0, 0], [np.nan] * 4], dtype=float
            ),
        ),
        "hurricane, r64 not forecast": (
            90.0,
            np.array(
                [[150, 120, 100, 130], [70, 60, 50, 55], [np.nan] * 4],
                dtype=float,
            ),
        ),
        "asymmetric, no 64 kt in two quadrants": (
            80.0,
            np.array(
                [[140, 100, 80, 110], [60, 40, 30, 40], [25, 20, 0, 0]],
                dtype=float,
            ),
        ),
    }

    failures = []
    for name, (vmax, radii) in cases.items():
        # 1. radii recovery
        for li, level in enumerate(RAD_LEVELS):
            if level >= vmax:
                continue
            for qi, brg in enumerate(QUAD_BEARINGS):
                r_nm = radii[li, qi]
                if not np.isfinite(r_nm) or r_nm <= 0:
                    continue
                got = probe(vmax, radii, r_nm * NM_TO_KM, brg)
                if abs(got - level) > 0.5:
                    failures.append(
                        f"{name}: r{level} quadrant {qi} -> {got:.1f} kt"
                    )

        # 2. never above Vmax, 3. monotonic outside Rmw
        rmw = rmw_climo_km(vmax, lat)
        rs = np.linspace(1, 500, 300)
        prof = np.array([probe(vmax, radii, r) for r in rs])
        if prof.max() > vmax + 0.5:
            failures.append(
                f"{name}: peak {prof.max():.1f} kt exceeds Vmax {vmax}"
            )
        outside = prof[rs > rmw * 1.05]
        if np.any(np.diff(outside) > 0.5):
            failures.append(f"{name}: wind increases outward beyond Rmw")

        if verbose:
            print(f"  {name}: peak {prof.max():.1f} kt (Vmax {vmax:.0f})")

    if failures:
        raise AssertionError(
            "windfield selfcheck failed:\n  " + "\n  ".join(failures)
        )
    if verbose:
        print(f"windfield selfcheck: {len(cases)} cases OK")
    return True


def interpolate_track(df, step_hours=1.0):
    """Resample one issuance's forecast track to a fixed time step.

    Linear in lat/lon/Vmax and in each quadrant radius, so the swath is
    continuous rather than a string of 6-hourly discs.
    """
    d = df.sort_values("leadtime").reset_index(drop=True)
    if len(d) < 2:
        return d

    lt = d["leadtime"].to_numpy(dtype=float)
    new_lt = np.arange(lt.min(), lt.max() + 1e-9, step_hours)

    out = {"leadtime": new_lt}
    for col in ("lat", "lon", "wind_speed"):
        out[col] = np.interp(new_lt, lt, d[col].to_numpy(dtype=float))

    for level in RAD_LEVELS:
        stack = np.stack(
            [parse_quad(v) for v in d[f"quadrant_radius_{level}"]]
        )  # (n_time, 4)
        interp = np.empty((len(new_lt), 4))
        for q in range(4):
            col = stack[:, q]
            ok = ~np.isnan(col)
            if ok.sum() == 0:
                interp[:, q] = np.nan
            elif ok.sum() == 1:
                interp[:, q] = np.where(
                    (new_lt >= lt[ok].min() - 6)
                    & (new_lt <= lt[ok].max() + 6),
                    col[ok][0],
                    np.nan,
                )
            else:
                interp[:, q] = np.interp(
                    new_lt, lt[ok], col[ok], left=np.nan, right=np.nan
                )
        out[f"quadrant_radius_{level}"] = list(interp)

    res = pd.DataFrame(out)
    res["issued_time"] = d["issued_time"].iloc[0]
    res["atcf_id"] = d["atcf_id"].iloc[0]
    res["valid_time"] = res["issued_time"] + pd.to_timedelta(
        res["leadtime"], unit="h"
    )
    return res


def max_wind_field(track, grid_lat, grid_lon, max_dist_km=800, rmw_scale=1.0):
    """Maximum wind (kt) at each grid point over a whole forecast track."""
    out = np.zeros(np.shape(grid_lat))
    for _, row in track.iterrows():
        # Skip track points far enough away to be irrelevant.
        if (
            haversine_km(
                row["lat"], row["lon"], np.mean(grid_lat), np.mean(grid_lon)
            )
            > max_dist_km
        ):
            continue
        radii = np.stack(
            [
                parse_quad(row[f"quadrant_radius_{level}"])
                for level in RAD_LEVELS
            ]
        )
        w = wind_at_points(
            row["wind_speed"],
            row["lat"],
            row["lon"],
            radii,
            grid_lat,
            grid_lon,
            rmw_scale=rmw_scale,
        )
        out = np.maximum(out, w)
    return out


if __name__ == "__main__":
    selfcheck()
