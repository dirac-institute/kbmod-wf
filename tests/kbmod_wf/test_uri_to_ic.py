"""Exercise URI builds through band selection and output writing."""

import importlib.util
from pathlib import Path
from unittest.mock import Mock

from astropy.table import Table
import numpy as np
import pytest


@pytest.fixture
def task():
    path = Path(__file__).parents[2] / "src/kbmod_wf/task_impls/uri_to_ic.py"
    spec = importlib.util.spec_from_file_location("uri_to_ic_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Collection:
    def __init__(self, bands):
        self.data = Table({"band": bands}) if bands is not None else Table({"id": [1]})
        self.write = Mock()

    def __len__(self):
        return len(self.data)

    def drop_bands(self, bands):
        self.data = self.data[~np.isin(self.data["band"], bands)]


@pytest.mark.parametrize("config,expected", [({}, ["u", "r"]), ({"bands_to_drop": []}, ["u", "y", "r"])])
def test_written_selection(task, tmp_path, monkeypatch, config, expected):
    source = tmp_path / "image.fits"
    source.touch()
    uris = tmp_path / "uris.txt"
    uris.write_text(f"# metadata\nfile://{source}\n\n")
    ic = Collection(["u", "y", "r"])
    monkeypatch.setattr(task.ImageCollection, "fromTargets", lambda paths: ic)
    task.uri_to_ic(str(uris), ic_filepath="output.collection", runtime_config=config, logger=Mock())
    assert list(ic.data["band"]) == expected
    ic.write.assert_called_once()


def test_all_y_fails_before_write(task, tmp_path, monkeypatch):
    uris = tmp_path / "uris.txt"
    uris.write_text("")
    ic = Collection(["y"])
    monkeypatch.setattr(task.ImageCollection, "fromTargets", lambda paths: ic)
    with pytest.raises(ValueError, match="No images remain"):
        task.uri_to_ic(str(uris), logger=Mock())
    ic.write.assert_not_called()


def test_missing_band_warns_and_retains_rows(task, tmp_path, monkeypatch):
    uris = tmp_path / "uris.txt"
    uris.write_text("")
    ic = Collection(None)
    logger = Mock()
    monkeypatch.setattr(task.ImageCollection, "fromTargets", lambda paths: ic)
    task.uri_to_ic(str(uris), logger=logger)
    assert len(ic) == 1
    logger.warning.assert_called_once()
    ic.write.assert_called_once()
