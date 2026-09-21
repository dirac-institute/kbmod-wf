import json
from pathlib import Path

import numpy as np
import pytest
from astropy.io import fits
from astropy.time import Time
from astropy.wcs import WCS
from kbmod_wf.task_impls.deep_validation import prepare, cut


def native_fixture(tmp_path):
    w = WCS(naxis=2)
    w.wcs.crpix = [5.0, 5.0]
    w.wcs.crval = [359.99, -2.0]
    w.wcs.cdelt = [-0.263 / 3600, 0.263 / 3600]
    w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    mask = np.zeros((10, 10), dtype=np.int32)
    mask[3, 3] = 1 << 10
    mask[4, 4] = (1 << 10) | (1 << 1)
    primary = fits.PrimaryHDU()
    primary.header.update(EXPNUM=100, CCDNUM=5, TIMESYS="TAI")
    primary.header["DATE-AVG"] = "2022-08-24T05:00:00"
    mh = fits.Header({"MP_BAD": 0, "MP_SAT": 1, "MP_NO_DATA": 8, "MP_FAKE": 10})
    psf = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="image", format="E", array=[[1.0]]),
            fits.Column(name="dimensions_x", format="J", array=[1]),
            fits.Column(name="dimensions_y", format="J", array=[1]),
        ],
        name="FixedKernel",
    )
    phot = fits.BinTableHDU.from_columns(
        [fits.Column(name="calibrationMean", format="D", array=[2.0])],
        name="PhotoCalib",
    )
    path = tmp_path / "native.fits"
    fits.HDUList(
        [
            primary,
            fits.ImageHDU(np.ones((10, 10)), header=w.to_header(), name="IMAGE"),
            fits.ImageHDU(np.ones((10, 10)), name="VARIANCE"),
            fits.ImageHDU(mask, header=mh, name="MASK"),
            psf,
            phot,
        ]
    ).writeto(path)
    t = Time(primary.header["DATE-AVG"], scale="tai").utc.mjd
    spec = dict(
        id="test",
        size=8,
        detector=5,
        frames=[dict(path=str(path), visit=100, mjd_utc=t, x0=1, y0=1)],
    )
    return spec


def test_keep_injection_pixels_and_reject_real_bad_pixels(tmp_path):
    spec = native_fixture(tmp_path)
    out = tmp_path / "test.npz"
    prepare(spec, {"mask_growth": 1, "mask_injections": False}, str(out))
    with np.load(out) as a:
        assert not a["mask"][0, 2, 2]  # FAKE alone is usable.
        assert a["mask"][0, 3, 3]  # FAKE+SAT remains rejected.
        np.testing.assert_allclose(a["science"], 2.0)
        np.testing.assert_allclose(a["variance"], 4.0)
    meta = json.loads(out.with_suffix(".json").read_text())
    assert meta["frames"][0]["injection_flagged_pixels_retained"] == 1
    assert meta["frames"][0]["injection_flagged_pixels"] == 2


def test_injection_mask_control_is_explicit(tmp_path):
    spec = native_fixture(tmp_path)
    out = tmp_path / "masked.npz"
    prepare(spec, {"mask_growth": 1, "mask_injections": True}, str(out))
    with np.load(out) as a:
        assert a["mask"][0, 2, 2]
    with pytest.raises(ValueError, match="Injection flags conflict"):
        prepare(spec, {"mask_flags": ["FAKE"], "mask_injections": False}, str(out))


def test_outside_detector_remains_invalid(tmp_path):
    spec = native_fixture(tmp_path)
    spec["frames"][0].update(x0=-2, y0=-2)
    out = tmp_path / "edge.npz"
    prepare(spec, {"mask_growth": 1}, str(out))
    with np.load(out) as a:
        assert a["mask"][0, :2, :].all()
        assert a["mask"][0, :, :2].all()
        assert np.isnan(a["science"][0, 0, 0])


def test_wrong_exposure_identity_fails(tmp_path):
    spec = native_fixture(tmp_path)
    spec["frames"][0]["visit"] = 101
    with pytest.raises(ValueError, match="identity"):
        prepare(spec, {}, str(tmp_path / "bad.npz"))
