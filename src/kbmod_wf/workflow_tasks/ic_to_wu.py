from parsl import python_app
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(
    cache=True, executors=get_executors(["local_dev_testing", "large_mem"]), ignore_for_cache=["logging_file"]
)
def ic_to_wu(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    import traceback
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.ic_to_wu import ic_to_wu

    logger = configure_logger("task.ic_to_wu", logging_file.filepath)

    logger.info("Starting ic_to_wu")
    try:
        ic_to_wu(
            ic_filepath=inputs[0].filepath,
            wu_filepath=outputs[0].filepath,
            runtime_config=runtime_config,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Error running ic_to_wu: {e}")
        logger.error(traceback.format_exc())
        raise e
    logger.warning("Completed ic_to_wu")

    return outputs[0]
