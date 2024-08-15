from parsl import python_app
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "sharded_reproject"]),
    ignore_for_cache=["logging_file"],
)
def reproject_wu(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    import traceback
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.reproject_multi_chip_multi_night_wu import reproject_wu

    logger = configure_logger("task.reproject_wu", logging_file.filepath)

    logger.info("Starting reproject_ic")
    try:
        reproject_wu(
            original_wu_filepath=inputs[0].filepath,
            reprojected_wu_filepath=outputs[0].filepath,
            runtime_config=runtime_config,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Error running reproject_ic: {e}")
        logger.error(traceback.format_exc())
        raise e
    logger.warning("Completed reproject_ic")

    return outputs[0]
