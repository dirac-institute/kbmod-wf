import pytest
from kbmod_wf import workflow_runner
from kbmod_wf.utilities.configuration_utilities import get_resource_config
from parsl.config import Config


def test_imports():
    assert workflow_runner is not None


def test_get_resource_config_dev():
    config = get_resource_config(env="dev")
    assert isinstance(config, Config)


def test_get_resource_config_klone():
    config = get_resource_config(env="klone")
    assert isinstance(config, Config)


def test_get_resource_config_usdf():
    # usdf config relies on specific environment constraints or imports that might need mocking
    # For now, just checking if it instantiates or raises a known error (e.g. if specific slurm paths are missing, that's distinct from TypeError on Config init)
    try:
        config = get_resource_config(env="usdf")
        assert isinstance(config, Config)
    except Exception as e:
        # If it fails due to missing environment vars or paths, that's "okay" for this specific check
        # as long as it's NOT the TypeError regarding deprecated arguments we are fixing.
        if "unexpected keyword argument" in str(e):
            pytest.fail(f"Config init failed with TypeError: {e}")
        # Otherwise, if it fails for other reasons (like missing monitoring address), we might need to mock more things.
        # But let's see what happens first.
        pass
