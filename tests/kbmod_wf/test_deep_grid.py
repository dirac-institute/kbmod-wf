"""Numerical guarantees for the selected grid; no GPU recovery claims."""
import math
import numpy as np
import pytest
from scipy.spatial import cKDTree
from kbmod_wf.deep_grid import grid_fragment


@pytest.mark.parametrize('baseline,sigma', [(7.615/24, 2.0), (5/24, 1.19), (1/24, 3.0)])
def test_nearest_vector_endpoint_error(baseline, sigma):
    record = grid_fragment(baseline, [sigma])
    c = record['kbmod_config']['generator_config']
    v = np.linspace(*c['velocities'])
    a = np.deg2rad(np.linspace(*c['angles']))
    nodes = np.stack([np.outer(np.cos(a), v).ravel(), np.outer(np.sin(a), v).ravel()], axis=1)
    # Cell centers are much harder than rechecking emitted grid nodes.
    vv = (v[1:]+v[:-1])/2
    aa = (a[1:]+a[:-1])/2
    truth = np.stack([np.outer(np.cos(aa), vv).ravel(), np.outer(np.sin(aa), vv).ravel()], axis=1)
    errors = cKDTree(nodes).query(truth)[0]*baseline
    assert errors.max() <= record['endpoint_drift_budget_pixels']*(1+1e-12)
    assert record['endpoint_drift_bound_pixels'] <= record['endpoint_drift_budget_pixels']*(1+1e-12)
    assert c['given_ecliptic'] is None
    assert record['kbmod_config']['lh_level'] == 5


@pytest.mark.parametrize('baseline,sigmas', [(0,[2]),(-1,[2]),(math.nan,[2]),(.1,[]),(.1,[0]),(.1,[2,math.nan])])
def test_missing_science_metadata_fails(baseline, sigmas):
    with pytest.raises(ValueError):
        grid_fragment(baseline, sigmas)
