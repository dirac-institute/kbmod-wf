from parsl import python_app
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(
    cache=True, executors=get_executors(["local_dev_testing", "large_mem"]), ignore_for_cache=["logging_file"]
)
def ic_to_wu(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    """This app will call the ic_to_wu function to convert a given ImageCollection
    file into a WorkUnit file.

    Parameters
    ----------
    inputs : tuple, optional
        A tuple with a single parsl.File object that references the ImageCollection
        file, by default ()
    outputs : tuple, optional
        A tuple with a single parsl.File object that references the output WorkUnit
        file, by default ()
    runtime_config : dict, optional
        A dictionary of configuration setting specific to this task, by default {}
    logging_file : parsl.File, optional
        The parsl.File object the defines where the logs are written, by default None

    Returns
    -------
    parsl.File
        The file object that points to the WorkUnit file that was created.
    """
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger

    logger = get_configured_logger("task.ic_to_wu", logging_file)

    from kbmod_wf.task_impls.ic_to_wu import ic_to_wu

    logger.info("Starting ic_to_wu")
    with ErrorLogger(logger):
        ic_to_wu(
            ic_filepath=inputs[0].filepath,
            wu_filepath=outputs[0].filepath,
            runtime_config=runtime_config,
            logger=logger,
        )
    logger.info("Completed ic_to_wu")

    return outputs[0]
