"""Verify runtime injection options without requiring KBMOD or the LSST stack."""

import importlib.util
import logging
import sys
from pathlib import Path
from types import ModuleType
from unittest import mock

import pytest
import toml


@pytest.fixture
def injection_module(monkeypatch):
    """Load the task with stand-ins for its optional scientific dependencies."""
    modules = {}
    for name, attributes in {
        "pandas": [],
        "astropy.table": ["Table"],
        "kbmod": ["ImageCollection"],
        "kbmod.configuration": ["SearchConfiguration"],
        "kbmod.injection": ["generate_injection_catalog", "inject_sources_into_ic"],
        "lsst.daf.butler": ["Butler"],
    }.items():
        modules[name] = ModuleType(name)
        for attribute in attributes:
            setattr(modules[name], attribute, mock.Mock())

    path = Path(__file__).resolve().parents[2] / "src/kbmod_wf/task_impls/ic_to_wu.py"
    spec = importlib.util.spec_from_file_location("injection_task_under_test", path)
    module = importlib.util.module_from_spec(spec)
    for name, replacement in modules.items():
        monkeypatch.setitem(sys.modules, name, replacement)
    spec.loader.exec_module(module)
    module._validate_injected_mask_support = mock.Mock()
    module._load_catalog_from_mapping = mock.Mock()
    module.inject_sources_into_ic.return_value = (mock.sentinel.injected_ic, mock.sentinel.injected_catalog)
    return module


@pytest.mark.parametrize(
    "zero_background,reduce_variance,constant_variance",
    [
        (False, False, False),
        (True, False, False),
        (False, True, False),
        (True, True, False),
        (False, False, True),
        (True, False, True),
    ],
)
@pytest.mark.parametrize("precomputed", [False, True])
def test_runtime_toml_options_reach_injector(
    injection_module, zero_background, reduce_variance, constant_variance, precomputed, caplog
):
    """Both catalog paths forward the requested booleans and fixed reduction factor."""
    # Parse real TOML, then select the app subsection passed by the workflow.
    runtime = toml.loads(
        "[apps.ic_to_wu.injection]\n"
        f"zero_background = {str(zero_background).lower()}\n"
        f"reduce_variance = {str(reduce_variance).lower()}\n"
        f"constant_variance = {str(constant_variance).lower()}\n"
    )["apps"]["ic_to_wu"]
    if precomputed:
        runtime["injection"]["catalog_mapping_path"] = "mapping.parquet"
        catalog = injection_module._load_catalog_from_mapping.return_value
    else:
        catalog = injection_module.generate_injection_catalog.return_value

    ic, butler = mock.Mock(), mock.Mock()
    with caplog.at_level(logging.INFO):
        result = injection_module.ic_to_injected_ic(
            ic, butler, runtime, 40.0, "input.ecsv", logger=logging.getLogger("test.injection")
        )

    # Disabled options must be omitted entirely to preserve the legacy KBMOD call.
    options = {}
    if zero_background:
        options["zero_background"] = True
    if constant_variance:
        options["constant_variance"] = True
        assert "constant variance planes of 1.0" in caplog.text
    elif reduce_variance:
        options["variance_scale"] = 1e-4
        assert "variance_scale=0.0001" in caplog.text
    else:
        assert "no variance scaling requested" in caplog.text
    injection_module.inject_sources_into_ic.assert_called_once_with(
        ic, catalog=catalog, butler=butler, **options
    )
    catalog.to_pandas.return_value.to_parquet.assert_called_once_with(
        "input.ecsv.injection_input_cat.parquet"
    )
    assert result == (mock.sentinel.injected_ic, mock.sentinel.injected_catalog)


@pytest.mark.parametrize("options", [{}, {"catalog_magnitude_mode": "native"}])
def test_default_options_preserve_legacy_call(injection_module, options):
    """Omitting the new flags does not require the new KBMOD keyword arguments."""
    ic, butler = mock.Mock(), mock.Mock()
    injection_module.ic_to_injected_ic(ic, butler, {"injection": options}, 40.0, "input.ecsv")
    injection_module.inject_sources_into_ic.assert_called_once_with(
        ic, catalog=injection_module.generate_injection_catalog.return_value, butler=butler
    )


def test_conflicting_variance_options_rejected(injection_module):
    """Reject conflicting modes before loading any exposures or catalogs."""
    runtime = {"injection": {"reduce_variance": True, "constant_variance": True}}
    with pytest.raises(ValueError, match="cannot both be true"):
        injection_module.ic_to_injected_ic(None, None, runtime, 40.0, "input.ecsv")
    # Reject the configuration before mask validation can load an exposure.
    injection_module._validate_injected_mask_support.assert_not_called()
    injection_module.inject_sources_into_ic.assert_not_called()


def test_total_mode_prepares_before_saving_and_injecting(injection_module, monkeypatch, caplog):
    """Persist and inject the prepared catalog, not the original total magnitudes."""
    prepare = mock.Mock(return_value=mock.Mock())
    monkeypatch.setattr(sys.modules["kbmod.injection"], "prepare_injection_catalog", prepare, raising=False)
    catalog = injection_module._load_catalog_from_mapping.return_value
    prepared = prepare.return_value
    runtime = toml.loads(
        '[apps.ic_to_wu.injection]\ncatalog_mapping_path = "mapping.parquet"\n'
        'catalog_magnitude_mode = "total"\n'
    )["apps"]["ic_to_wu"]
    with caplog.at_level(logging.INFO):
        injection_module.ic_to_injected_ic(
            mock.sentinel.ic,
            mock.sentinel.butler,
            runtime,
            40.0,
            "input.ecsv",
            logger=logging.getLogger("test.injection"),
        )
    prepare.assert_called_once_with(catalog)
    prepared.to_pandas.return_value.to_parquet.assert_called_once_with(
        "input.ecsv.injection_input_cat.parquet"
    )
    catalog.to_pandas.assert_not_called()
    injection_module.inject_sources_into_ic.assert_called_once_with(
        mock.sentinel.ic, catalog=prepared, butler=mock.sentinel.butler
    )
    injection_module.generate_injection_catalog.assert_not_called()
    assert "preserving total magnitudes as truth" in caplog.text


@pytest.mark.parametrize("value", ["TOTAL", "unknown", None, True, 1, []])
def test_invalid_magnitude_mode_rejected(injection_module, value):
    """Reject invalid magnitude conventions before loading exposures."""
    with pytest.raises(ValueError, match="catalog_magnitude_mode must be"):
        injection_module.ic_to_injected_ic(
            None, None, {"injection": {"catalog_magnitude_mode": value}}, 40.0, "input.ecsv"
        )
    injection_module._validate_injected_mask_support.assert_not_called()


def test_total_mode_requires_mapping(injection_module):
    """Total magnitudes apply only to an explicitly supplied external catalog."""
    with pytest.raises(ValueError, match="requires catalog_mapping_path"):
        injection_module.ic_to_injected_ic(
            None, None, {"injection": {"catalog_magnitude_mode": "total"}}, 40.0, "input.ecsv"
        )
    injection_module._validate_injected_mask_support.assert_not_called()


def test_total_mode_requires_new_kbmod(injection_module):
    """Old KBMOD remains usable in native mode and reports an actionable total-mode error."""
    runtime = {"injection": {"catalog_mapping_path": "mapping.parquet", "catalog_magnitude_mode": "total"}}
    with pytest.raises(RuntimeError, match="requires a KBMOD version with prepare_injection_catalog"):
        injection_module.ic_to_injected_ic(None, None, runtime, 40.0, "input.ecsv")
    injection_module._validate_injected_mask_support.assert_not_called()
    injection_module._load_catalog_from_mapping.assert_not_called()


def test_total_mode_does_not_fall_back_to_generation(injection_module, monkeypatch):
    """Missing mappings/files must not silently replace the requested external population."""
    prepare = mock.Mock()
    monkeypatch.setattr(sys.modules["kbmod.injection"], "prepare_injection_catalog", prepare, raising=False)
    injection_module._load_catalog_from_mapping.return_value = None
    runtime = {"injection": {"catalog_mapping_path": "mapping.parquet", "catalog_magnitude_mode": "total"}}
    with pytest.raises(ValueError, match="requires a mapped catalog for input.ecsv"):
        injection_module.ic_to_injected_ic(mock.Mock(), mock.Mock(), runtime, 40.0, "input.ecsv")
    prepare.assert_not_called()
    injection_module.generate_injection_catalog.assert_not_called()
    injection_module.inject_sources_into_ic.assert_not_called()


def test_total_mode_with_real_kbmod_helper(tmp_path, monkeypatch, request):
    """When new KBMOD is available, verify mixed-source values and saved truth through the adapter."""
    # Import before the stand-ins are installed. Standard CI needs neither
    # KBMOD nor LSST; run this additional integration check with KBMOD PR #1189.
    kbmod_injection = pytest.importorskip("kbmod.injection")
    if not hasattr(kbmod_injection, "prepare_injection_catalog"):
        pytest.skip("Requires KBMOD prepare_injection_catalog")
    import numpy as np
    import pandas as pd
    import astropy.table as table_module

    injection_module = request.getfixturevalue("injection_module")
    monkeypatch.setitem(sys.modules, "kbmod.injection", kbmod_injection)
    monkeypatch.setitem(sys.modules, "pandas", pd)
    monkeypatch.setitem(sys.modules, "astropy.table", table_module)
    truth = table_module.Table(
        {
            "source_type": ["Star", "Trail"],
            "mag_total": [22.0, 22.0],
            "obj_ids": [10, 11],
            "obstime": [59000.0, 59000.0],
        }
    )
    truth["trail_length"] = table_module.MaskedColumn([0.0, 10.0], mask=[True, False])
    truth["beta"] = table_module.MaskedColumn([0.0, 45.0], mask=[True, False])
    injection_module._load_catalog_from_mapping.return_value = truth
    ic_path = str(tmp_path / "input.ecsv")
    runtime = {"injection": {"catalog_mapping_path": "mapping.parquet", "catalog_magnitude_mode": "total"}}
    injection_module.ic_to_injected_ic(mock.Mock(), mock.Mock(), runtime, 40.0, ic_path)
    prepared = injection_module.inject_sources_into_ic.call_args.kwargs["catalog"]
    saved = pd.read_parquet(ic_path + ".injection_input_cat.parquet")
    np.testing.assert_allclose(prepared["mag"], [22.0, 24.5])
    np.testing.assert_allclose(saved["mag"], prepared["mag"])
    np.testing.assert_array_equal(saved["mag_total"], truth["mag_total"])
    np.testing.assert_array_equal(saved["obj_ids"], truth["obj_ids"])
    np.testing.assert_array_equal(saved["obstime"], truth["obstime"])
    assert pd.isna(saved["beta"][0])
    assert saved["beta"][1] == 45.0
    assert "mag" not in truth.colnames


@pytest.mark.parametrize("name", ["zero_background", "reduce_variance", "constant_variance"])
@pytest.mark.parametrize("value", ["false", 1, None])
def test_non_boolean_options_rejected(injection_module, name, value):
    """Reject truthy strings and numbers before loading or changing exposures."""
    with pytest.raises(ValueError, match=f"injection.{name} must be a boolean"):
        injection_module.ic_to_injected_ic(None, None, {"injection": {name: value}}, 40.0, "input.ecsv")
    injection_module._validate_injected_mask_support.assert_not_called()
    injection_module.inject_sources_into_ic.assert_not_called()
