import platform
from typing import Literal

from kbmod_wf.configurations import *


def get_config(env: Literal["dev", "klone"] | None = None):
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
            config = dev_config()
        else:
            config = klone_config()
    elif env == "dev":
        config = dev_config()
    elif env == "klone":
        config = klone_config()
    else:
        raise ValueError(f"Unknown environment: {env}")

    return config
