"""Original-sky DEEP grid policy, independent of Parsl site configuration.

This does not alter the earlier targeted-recovery workflow. A blind-search
caller must persist the returned fragment alongside its remaining search settings.
"""

import math

GRID_ID = "deep-original-tno-v1"
SPEED_BOUNDS = (5.0, 600.0)  # Native DECam-scale common WCS, pixels/day.
ANGLE_BOUNDS = (120.0, 240.0)  # Offset from positive ecliptic longitude, degrees.


def grid_fragment(baseline_days, psf_sigmas_pixels):
    """Return LH/generator fields and the numerical grid error budget.

    PSF sigmas must describe all searched exposures in the common pixel frame.
    Spacing limits combined endpoint displacement from velocity quantization to
    min(1 pixel, half the sharpest PSF sigma). This excludes start-position
    quantization, nonlinear motion, WCS errors, and all subsequent filtering.
    """
    if not math.isfinite(baseline_days) or baseline_days <= 0:
        raise ValueError("A positive finite time baseline is required")
    sigmas = list(psf_sigmas_pixels)
    if not sigmas or any(not math.isfinite(s) or s <= 0 for s in sigmas):
        raise ValueError("Positive finite PSF sigmas for all exposures are required")
    drift = min(1.0, 0.5 * min(sigmas))
    dv_max = math.sqrt(2.0) * drift / baseline_days
    da_max = dv_max / SPEED_BOUNDS[1]  # radians; chord length <= arc length
    nv = max(2, math.ceil((SPEED_BOUNDS[1] - SPEED_BOUNDS[0]) / dv_max) + 1)
    na = max(2, math.ceil(math.radians(ANGLE_BOUNDS[1] - ANGLE_BOUNDS[0]) / da_max) + 1)
    dv = (SPEED_BOUNDS[1] - SPEED_BOUNDS[0]) / (nv - 1)
    da = math.radians(ANGLE_BOUNDS[1] - ANGLE_BOUNDS[0]) / (na - 1)
    bound = baseline_days * math.hypot(dv / 2, SPEED_BOUNDS[1] * da / 2)
    return {
        "grid_id": GRID_ID,
        "baseline_days": float(baseline_days),
        "min_psf_sigma_pixels": float(min(sigmas)),
        "endpoint_drift_budget_pixels": drift,
        "endpoint_drift_bound_pixels": bound,
        "vectors_per_pixel": nv * na,
        "kbmod_config": {
            "lh_level": 5.0,
            "candidate_dup_px": 0,
            "generator_config": {
                "name": "EclipticCenteredSearch",
                "velocities": [*SPEED_BOUNDS, nv],
                "angles": [*ANGLE_BOUNDS, na],
                "angle_units": "degree",
                "velocity_units": "pix / d",
                "given_ecliptic": None,
            },
        },
    }


def make_workunit_grid(work_unit, psf_sigmas_pixels, *, frame):
    """Build a native-sky generator with explicit frame, timing and WCS guards.

    WCS/scale and PSF values must correspond to the aligned WorkUnit, not the
    input detector before resampling. This policy is intentionally not an EBD grid.
    """
    import numpy as np
    from astropy.wcs.utils import proj_plane_pixel_scales
    from kbmod.trajectory_generator import EclipticCenteredSearch

    if frame != "original":
        raise ValueError("The DEEP original-sky grid cannot be used in a reflex frame")
    wcs = work_unit.get_wcs(0)
    if wcs is None or not wcs.has_celestial:
        raise ValueError("A celestial common WCS is required")
    scales = proj_plane_pixel_scales(wcs.celestial) * 3600
    if not np.all(np.isfinite(scales)) or not np.all((scales >= 0.25) & (scales <= 0.28)):
        raise ValueError("This grid requires a DECam-scale WCS (0.25-0.28 arcsec/pixel)")
    angle = work_unit.compute_ecliptic_angle()
    if angle is None or not np.isfinite(angle):
        raise ValueError("Unable to derive a finite ecliptic angle from the WorkUnit")
    times = np.asarray(work_unit.get_all_obstimes(), dtype=float)
    sigmas = list(psf_sigmas_pixels)
    if times.ndim != 1 or len(times) < 2 or not np.all(np.isfinite(times)):
        raise ValueError("At least two finite exposure midpoints are required")
    if len(sigmas) != len(times):
        raise ValueError("One PSF sigma per exposure is required")
    record = grid_fragment(float(np.ptp(times)), sigmas)
    generator = EclipticCenteredSearch(work_unit=work_unit, **record["kbmod_config"]["generator_config"])
    record["resolved_ecliptic_angle_radians"] = float(angle)
    record["pixel_scales_arcsec"] = scales.tolist()
    return record, generator
