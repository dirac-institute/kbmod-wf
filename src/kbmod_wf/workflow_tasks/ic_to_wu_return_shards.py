from parsl import python_app
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(
    cache=True, executors=get_executors(["local_dev_testing", "large_mem"]), ignore_for_cache=["logging_file"]
)
def ic_to_wu_return_shards(inputs=(), outputs=(), runtime_config={}, logging_file=None):
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
    from pathlib import Path
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger

    logger = get_configured_logger("task.ic_to_wu", logging_file)

    from kbmod_wf.task_impls.ic_to_wu import ic_to_wu

    logger.info("Starting ic_to_wu")
    with ErrorLogger(logger):
        _, wcs = ic_to_wu(
            ic_filepath=inputs[0].filepath,
            wu_filepath=outputs[0].filepath,
            runtime_config=runtime_config,
            logger=logger,
        )
    logger.info("Completed ic_to_wu")

    # get parent directory of outputs[0] and fine all .wu files in that directory
    shard_files = [s for s in Path(outputs[0]).parent.glob("*.wu")]

    # remove the original .wu file from the shard_files list
    shard_files = [f for f in shard_files if f != outputs[0]]

    return shard_files, wcs
