from parsl import python_app
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "sharded_reproject"]),
    ignore_for_cache=["logging_file"],
)
def reproject_wu(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    """This app will call the reproject_wu function to reproject and reflex correct
    a given WorkUnit file.

    Parameters
    ----------
    inputs : tuple, optional
        A tuple with a single parsl.File object that references the original WorkUnit
        file, by default ()
    outputs : tuple, optional
        A tuple with a single parsl.File object that references the reprojected
        WorkUnit file, by default ()
    runtime_config : dict, optional
        A dictionary of configuration setting specific to this task, by default {}
    logging_file : parsl.File, optional
        The parsl.File object the defines where the logs are written, by default None

    Returns
    -------
    parsl.File
        The file object that points to the resulting WorkUnit file that was created.

    Raises
    ------
    e
        Reraises any exceptions that occur during the execution of the reproject_wu
        function.
    """
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger

    logger = get_configured_logger("task.ic_to_wu", logging_file)

    from kbmod_wf.task_impls.reproject_multi_chip_multi_night_wu_from_uris import reproject_wu

    logger.info("Starting reproject_ic")
    with ErrorLogger(logger):
        reproject_wu(
            original_wu_filepath=inputs[0].filepath,
            uri_filepath=inputs[1].filepath,
            reprojected_wu_filepath=outputs[0].filepath,
            runtime_config=runtime_config,
            logger=logger,
        )
    logger.info("Completed reproject_ic")

    return outputs[0]
