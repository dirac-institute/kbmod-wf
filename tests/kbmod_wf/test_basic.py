import pytest
from kbmod_wf import workflow_runner
from kbmod_wf.utilities.configuration_utilities import get_resource_config
from parsl.config import Config

def test_imports():
    assert workflow_runner is not None

def test_get_resource_config_dev():
    config = get_resource_config(env="dev")
    assert isinstance(config, Config)
