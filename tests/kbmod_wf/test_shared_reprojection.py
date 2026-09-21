def test_production_reflex_runner_calls_shared_helper(monkeypatch, tmp_path):
    import sys
    import types
    from unittest.mock import Mock
    from astropy.coordinates import EarthLocation
    from kbmod_wf.task_impls import reproject_multi_chip_multi_night_wu as module

    wu, projected, patch_wcs = Mock(), Mock(), Mock()
    adapter = types.ModuleType("kbmod_wf.task_impls.ic_to_wu")
    adapter.ic_to_wu = Mock(return_value=wu)
    monkeypatch.setitem(sys.modules, adapter.__name__, adapter)
    ic = Mock()
    ic.get_global_wcs.return_value = patch_wcs
    monkeypatch.setattr(module.ImageCollection, "read", Mock(return_value=ic))
    helper = Mock(return_value=projected)
    monkeypatch.setattr(module, "reproject_workunit_in_frame", helper)
    runner = module.WUReprojector.__new__(module.WUReprojector)
    runner.guess_dist, runner.ic_filepath = 42.0, "input.collection"
    runner.reprojected_wu_filepath = str(tmp_path / "test.wu")
    runner.runtime_config = {"ebd_fit_seed": 17}
    runner.logger, runner.overwrite, runner.n_workers = Mock(), False, 3
    runner.point_on_earth = EarthLocation.from_geodetic(-70.81489, -30.16606, 2215.0)
    assert runner.reproject_workunit() == str(tmp_path / "test.wu")
    helper.assert_called_once_with(wu, patch_wcs, guess_dist=42.0, n_workers=3, npoints=100, seed=17)
    assert wu.observatory is runner.point_on_earth
    projected.to_sharded_fits.assert_called_once_with("test.wu", str(tmp_path), overwrite=False)
