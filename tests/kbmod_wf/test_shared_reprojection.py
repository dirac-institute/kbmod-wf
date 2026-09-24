"""Verify the WorkUnit reprojection tasks call KBMOD without requiring KBMOD or the LSST stack."""

import importlib.util
import logging
import sys
from pathlib import Path
from types import ModuleType
from unittest import mock

import pytest

TASK_DIR = Path(__file__).resolve().parents[2] / "src/kbmod_wf/task_impls"


def _load_task(monkeypatch, filename):
    """Load a reprojection task with stand-ins for its KBMOD dependencies."""
    modules = {}
    for name, attributes in {
        "kbmod": ["ImageCollection", "_logging"],
        "kbmod.reprojection": ["reproject_work_unit_to_distance"],
        "kbmod.work_unit": ["WorkUnit"],
        "kbmod_wf.task_impls.ic_to_wu": ["ic_to_wu"],
    }.items():
        modules[name] = ModuleType(name)
        for attribute in attributes:
            setattr(modules[name], attribute, mock.Mock())
    modules["kbmod"].reprojection = modules["kbmod.reprojection"]

    spec = importlib.util.spec_from_file_location(f"{filename}_under_test", TASK_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    for name, replacement in modules.items():
        monkeypatch.setitem(sys.modules, name, replacement)
    spec.loader.exec_module(module)
    return module, modules


@pytest.fixture
def ic_task(monkeypatch):
    """The ImageCollection-based (Rubin) reprojection task."""
    return _load_task(monkeypatch, "reproject_multi_chip_multi_night_wu.py")


@pytest.mark.parametrize("site", [None, "ctio"])
def test_ic_reprojection_calls_kbmod(ic_task, site, tmp_path):
    """The task forwards the patch WCS and fit settings, and only overrides a configured site."""
    module, modules = ic_task
    wu = mock.Mock()
    original_observatory = wu.observatory
    modules["kbmod_wf.task_impls.ic_to_wu"].ic_to_wu.return_value = wu
    ic = module.ImageCollection.read.return_value
    resampled_wu = module.reprojection.reproject_work_unit_to_distance.return_value

    runtime_config = {"ebd_fit_seed": 17, "n_workers": 3, "overwrite": False}
    if site is not None:
        runtime_config["observation_site"] = site
    output_path = str(tmp_path / "test.wu")
    result = module.reproject_wu(
        42.0, "input.collection", output_path, runtime_config, logger=logging.getLogger("test")
    )

    assert result == output_path
    module.reprojection.reproject_work_unit_to_distance.assert_called_once_with(
        wu,
        42.0,
        common_wcs=ic.get_global_wcs.return_value,
        npoints=100,
        seed=17,
        parallelize=True,
        max_parallel_processes=3,
    )
    if site is None:
        assert wu.observatory is original_observatory
    else:
        assert wu.observatory == module.EarthLocation.of_site(site)
    resampled_wu.to_sharded_fits.assert_called_once_with("test.wu", str(tmp_path), overwrite=False)


def test_ic_reprojection_requires_global_wcs(ic_task, tmp_path):
    """Fail before reprojecting if the ImageCollection has no patch WCS."""
    module, _ = ic_task
    module.ImageCollection.read.return_value.get_global_wcs.return_value = None
    with pytest.raises(ValueError, match="No global WCS"):
        module.reproject_wu(
            42.0, "input.collection", str(tmp_path / "test.wu"), {}, logger=logging.getLogger("test")
        )
    module.reprojection.reproject_work_unit_to_distance.assert_not_called()


def test_uri_reprojection_calls_kbmod(monkeypatch, tmp_path):
    """The URI-based task reprojects the lazy WorkUnit onto the patch WCS at the URI distance."""
    module, _ = _load_task(monkeypatch, "reproject_multi_chip_multi_night_from_uris.py")
    uri_path = tmp_path / "test.uri"
    uri_path.write_text(
        "#dist_au=42.0\n"
        "#patch_size=[20, 20]\n"
        "#pixel_scale=0.263\n"
        "#patch_box=[[216.3, -13.7], [216.3, -13.3], [216.7, -13.3], [216.7, -13.7]]\n"
        "/path/to/image.fits\n"
    )
    wu = module.WorkUnit.from_sharded_fits.return_value

    output_path = str(tmp_path / "reprojected.wu")
    result = module.reproject_wu(
        original_wu_filepath=str(tmp_path / "original.wu"),
        uri_filepath=str(uri_path),
        reprojected_wu_filepath=output_path,
        runtime_config={"n_workers": 3},
        logger=logging.getLogger("test"),
    )

    assert result == output_path
    assert wu.observatory == module.EarthLocation.of_site("ctio")
    call = module.reprojection.reproject_work_unit_to_distance.call_args
    assert call.args == (wu, 42.0)
    assert call.kwargs["common_wcs"].array_shape == (4563, 4563)
    assert call.kwargs["directory"] == str(tmp_path)
    assert call.kwargs["filename"] == "reprojected.wu"
    assert call.kwargs["max_parallel_processes"] == 3
