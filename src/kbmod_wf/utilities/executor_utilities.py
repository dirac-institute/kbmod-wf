from kbmod_wf.utilities.configuration_utilities import get_resource_config


def get_executors(possible_executors=[]):
    """Get the list of executors that are available on the system.

    Parameters
    ----------
    possible_executors : List[str]
        A list of possible executors that can be used.

    Returns
    -------
    List[str]
        A list of executors that are available on the system.
    """

    config = get_resource_config()
    available_executors = [e.label for e in config.executors]

    return [executor for executor in possible_executors if executor in available_executors]
