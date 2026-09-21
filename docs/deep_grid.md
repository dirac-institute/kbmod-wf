# Adaptive original-sky DEEP grid

`deep-original-tno-v1` uses `EclipticCenteredSearch`, speeds 5–600 pixels/day,
angle offsets 120–240 degrees relative to increasing ecliptic longitude, LH 5,
and `candidate_dup_px=0`. This is a TNO motion-domain selection; it does not
cover every possible moving object or the fast asteroid population.

`grid_fragment(baseline_days, psf_sigmas_pixels)` sizes both axes so the
combined endpoint drift from velocity quantization is at most the smaller of
one pixel or half the minimum PSF sigma. Supply one PSF sigma per exposure
in the aligned common pixel frame. Starting-position rounding, curvature,
resampling, WCS errors, filtering and top-K losses are outside this bound.

`make_workunit_grid(work_unit, psf_sigmas_pixels, frame="original")` returns
the metadata/config record and a real KBMOD trajectory generator. It rejects
missing celestial WCS, non-DECam scale (outside 0.25–0.28 arcsec/pixel), invalid
timing, missing PSFs, or a reflex frame. Persist the returned record alongside
the complete final configuration. The example YAML is a fragment for a
7.614929-hour baseline and PSF sigma >=2 pixels, not a universal fixed grid.

For endpoint drift budget b and baseline T in days, the sample counts are
`ceil(595*T/(sqrt(2)*b))+1` in speed and
`ceil(radians(120)*600*T/(sqrt(2)*b))+1` in angle, each at least two.
Sharper PSFs and longer stacks require more samples. A denser-grid sensitivity
comparison is still needed before interpreting a recovery fraction.

This opt-in module does not change the targeted validation workflow or build
a full-frame campaign runner. Injection masking should remain off for existing
fake-source validation, with ordinary bad-pixel masks retained. The caller must
set minimum observations, results-per-pixel, filtering, checkpointing and output
caps explicitly. A reflex-corrected run needs a grid measured in that frame.
