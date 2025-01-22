import platform
import toml
from typing import Literal

from kbmod_wf.resource_configs import *


def get_resource_config(env: Literal["dev", "klone", "usdf"] | None = None):
    """A naive attempt to return a reasonable configuration using platform.system.
    This will likely be insufficient in a very short amount of time, but this
    is meant to be a first step.

    Parameters
    ----------
    env : Literal["dev", "klone", "usdf"] | None, optional
        The common name used to retrieve the given configuration, by default None.
        If none, the configuration will be determined by the platform.system().

    Returns
    -------
    parls.config.Config
        The configuration object to be used by the parsl.load() function.

    Raises
    ------
    ValueError
        If an unknown environment is provided, raise a ValueError.
    """

    if env is None:
        if platform.system().lower() == "darwin":
            config = dev_resource_config()
        elif is_running_on_wsl():
            config = dev_resource_config()
        else:
            config = klone_resource_config()
    elif env == "dev":
        config = dev_resource_config()
    elif env == "klone":
        config = klone_resource_config()
    elif env == "usdf":
        config = usdf_resource_config()
    else:
        raise ValueError(f"Unknown environment: {env}")

    return config


def is_running_on_wsl() -> bool:
    """Check if the script is running on Windows Subsystem for Linux (WSL)."""
    if platform.system().lower() == "linux":
        try:
            with open("/proc/version") as version_file:
                content = version_file.read().lower()
                return "microsoft" in content or "wsl" in content
        except FileNotFoundError:
            pass
    return False


def apply_runtime_updates(resource_config, runtime_config):
    """Before calling parsl.load(config), we want to modify any resource configuration
    parameters with any runtime configuration options that might be set.

    Any key in the top level of the runtime_config dictionary that matches a
    parameter of the resource_config will be used to override the resource_config
    value.

    Parameters
    ----------
    resource_config : parsl.config.Config
        The configuration object that defines the computational resources for
        running the workflow. These are defined in the resource_configs module.
    runtime_config : dict
        This is the set of runtime configuration options that are used to modify
        the workflow on a per-run basis.

    Returns
    -------
    parsl.config.Config
        The original resource_config updated with values from runtime_config
    """
    resource_config_modifiers = runtime_config.get("resource_config_modifiers", {})
    for key, value in resource_config_modifiers.items():
        setattr(resource_config, key, value)

    return resource_config
