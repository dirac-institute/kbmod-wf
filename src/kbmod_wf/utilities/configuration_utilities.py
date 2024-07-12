import platform
from typing import Literal

from kbmod_wf.resource_configs import *


def get_resource_config(env: Literal["dev", "klone"] | None = None):
    """A naive attempt to return a reasonable configuration using platform.system.
    This will likely be insufficient in a very short amount of time, but this
    is meant to be a first step.

    Parameters
    ----------
    env : Literal["dev", "klone"] | None, optional
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
