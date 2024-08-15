from parsl import python_app
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(
    cache=True, executors=get_executors(["local_dev_testing", "small_cpu"]), ignore_for_cache=["logging_file"]
)
def uri_to_ic(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    import traceback
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.uri_to_ic import uri_to_ic

    logger = configure_logger("task.uri_to_ic", logging_file.filepath)

    logger.info("Starting uri_to_ic")
    try:
        uri_to_ic(
            uris_filepath=inputs[0].filepath,
            uris_base_dir=None,  # determine what, if any, value should be used.
            ic_filepath=outputs[0].filepath,
            runtime_config=runtime_config,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"Error running uri_to_ic: {e}")
        logger.error(traceback.format_exc())
        raise e
    logger.warning("Completed uri_to_ic")

    return outputs[0]
