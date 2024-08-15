from parsl import python_app
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(
    cache=True, executors=get_executors(["local_dev_testing", "gpu"]), ignore_for_cache=["logging_file"]
)
def kbmod_search(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    import traceback
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.kbmod_search import kbmod_search

    logger = configure_logger("task.kbmod_search", logging_file.filepath)

    logger.info("Starting kbmod_search")
    try:
        kbmod_search(
            wu_filepath=inputs[0].filepath,
            result_filepath=outputs[0].filepath,
            runtime_config=runtime_config,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Error running kbmod_search: {e}")
        logger.error(traceback.format_exc())
        raise e
    logger.warning("Completed kbmod_search")

    return outputs[0]
