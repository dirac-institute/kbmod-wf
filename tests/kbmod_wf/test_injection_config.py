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
    module.inject_sources_into_ic.return_value = (
        mock.sentinel.injected_ic,
        mock.sentinel.injected_catalog,
    )
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
    injection_module,
    zero_background,
    reduce_variance,
    constant_variance,
    precomputed,
    caplog,
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
            ic,
            butler,
            runtime,
            40.0,
            "input.ecsv",
            logger=logging.getLogger("test.injection"),
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


def test_default_options_preserve_legacy_call(injection_module):
    """Omitting the new flags does not require the new KBMOD keyword arguments."""
    ic, butler = mock.Mock(), mock.Mock()
    injection_module.ic_to_injected_ic(ic, butler, {"injection": {}}, 40.0, "input.ecsv")
    injection_module.inject_sources_into_ic.assert_called_once_with(
        ic,
        catalog=injection_module.generate_injection_catalog.return_value,
        butler=butler,
    )


def test_conflicting_variance_options_rejected(injection_module):
    """Reject conflicting modes before loading any exposures or catalogs."""
    runtime = {"injection": {"reduce_variance": True, "constant_variance": True}}
    with pytest.raises(ValueError, match="cannot both be true"):
        injection_module.ic_to_injected_ic(None, None, runtime, 40.0, "input.ecsv")
    # Reject the configuration before mask validation can load an exposure.
    injection_module._validate_injected_mask_support.assert_not_called()
    injection_module.inject_sources_into_ic.assert_not_called()


@pytest.mark.parametrize("name", ["zero_background", "reduce_variance", "constant_variance"])
@pytest.mark.parametrize("value", ["false", 1, None])
def test_non_boolean_options_rejected(injection_module, name, value):
    """Reject truthy strings and numbers before loading or changing exposures."""
    with pytest.raises(ValueError, match=f"injection.{name} must be a boolean"):
        injection_module.ic_to_injected_ic(None, None, {"injection": {name: value}}, 40.0, "input.ecsv")
    injection_module._validate_injected_mask_support.assert_not_called()
    injection_module.inject_sources_into_ic.assert_not_called()


@pytest.mark.parametrize("name", ["injection_workers", "max_images_per_shard"])
@pytest.mark.parametrize("value", [0, -1, True, "2", 1.5, None])
def test_invalid_parallel_options_fail_before_loading(injection_module, name, value):
    converter = injection_module.ICtoWUConverter(
        runtime_config={"injection": {name: value}}, logger=mock.Mock()
    )
    with pytest.raises(ValueError, match=name):
        converter.create_work_unit()
    injection_module.ImageCollection.read.assert_not_called()
    injection_module.Butler.assert_not_called()


def test_parallel_requires_saved_output(injection_module):
    converter = injection_module.ICtoWUConverter(
        save=False,
        runtime_config={"injection": {"injection_workers": 2}},
        logger=mock.Mock(),
    )
    with pytest.raises(ValueError, match="save=True"):
        converter.create_work_unit()
    injection_module.ImageCollection.read.assert_not_called()


@pytest.mark.parametrize("precomputed", [False, True])
def test_parallel_workflow_writes_in_workers(injection_module, monkeypatch, precomputed):
    parallel = ModuleType("kbmod.injection_parallel")
    output_catalog = mock.Mock()
    parallel.inject_sources_to_workunit = mock.Mock(return_value=("output.fits", output_catalog))
    monkeypatch.setitem(sys.modules, "kbmod.injection_parallel", parallel)
    runtime = {
        "butler_config_filepath": "repo.yaml",
        "search_config_filepath": "search.yaml",
        "injection": {
            "injection_workers": 2,
            "max_images_per_shard": 4,
            "zero_background": True,
            "constant_variance": True,
        },
    }
    if precomputed:
        runtime["injection"]["catalog_mapping_path"] = "mapping.parquet"
        catalog = injection_module._load_catalog_from_mapping.return_value
    else:
        catalog = injection_module.generate_injection_catalog.return_value
    converter = injection_module.ICtoWUConverter(
        "input.ecsv", "output.fits", runtime_config=runtime, logger=mock.Mock()
    )
    assert converter.create_work_unit() == "output.fits"
    parallel.inject_sources_to_workunit.assert_called_once_with(
        injection_module.ImageCollection.read.return_value,
        catalog,
        "repo.yaml",
        "output.fits",
        injection_workers=2,
        max_images_per_shard=4,
        search_config=injection_module.SearchConfiguration.from_file.return_value,
        zero_background=True,
        constant_variance=True,
    )
    injection_module.inject_sources_into_ic.assert_not_called()
    injection_module.ImageCollection.read.return_value.toWorkUnit.assert_not_called()
    injection_module.Butler.return_value.close.assert_called_once()
    catalog.to_pandas.return_value.to_parquet.assert_called_once_with(
        "input.ecsv.injection_input_cat.parquet"
    )
    output_catalog.to_pandas.return_value.to_parquet.assert_called_once_with(
        "input.ecsv.injection_cat.parquet"
    )
    assert injection_module.generate_injection_catalog.call_count == (0 if precomputed else 1)


def test_direct_ic_helper_rejects_parallel_setting(injection_module):
    with pytest.raises(ValueError, match="ICtoWUConverter"):
        injection_module.ic_to_injected_ic(
            None, None, {"injection": {"injection_workers": 2}}, 40.0, "input.ecsv"
        )
    injection_module.generate_injection_catalog.assert_not_called()
