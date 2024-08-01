import argparse
import os
import toml
import parsl
from parsl import python_app, File
import parsl.executors

from kbmod_wf.utilities.configuration_utilities import apply_runtime_updates, get_resource_config
from kbmod_wf.utilities.executor_utilities import get_executors
from kbmod_wf.utilities.logger_utilities import configure_logger


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "local_thread"]),
    ignore_for_cache=["logging_file"],
)
def create_uri_manifest(inputs=[], outputs=[], runtime_config={}, logging_file=None):
    """This app will go to a given directory, find all of the uri.lst files there,
    and copy the paths to the manifest file."""
    import glob
    import os
    from kbmod_wf.utilities.logger_utilities import configure_logger

    logger = configure_logger("task.create_uri_manifest", logging_file.filepath)

    directory_path = runtime_config.get("staging_directory")
    if directory_path is None:
        raise ValueError("No staging_directory provided in the configuration.")

    logger.info(f"Looking for staged files in {directory_path}")

    # Gather all the *.lst entries in the directory
    pattern = os.path.join(directory_path, "*.lst")
    entries = glob.glob(pattern)

    # Filter out directories, keep only files
    files = []
    for f in entries:
        if os.path.isfile(os.path.join(directory_path, f)):
            files.append(os.path.join(os.path.abspath(directory_path), f))

    logger.info(f"Found {len(files)} files in {directory_path}")

    # Write the filenames to the manifest file
    with open(outputs[0].filename, "w") as manifest_file:
        for file in files:
            manifest_file.write(file + "\n")

    return outputs[0]


@python_app(
    cache=True, executors=get_executors(["local_dev_testing", "small_cpu"]), ignore_for_cache=["logging_file"]
)
def uri_to_ic(inputs=[], outputs=[], runtime_config={}, logging_file=None):
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


@python_app(
    cache=True, executors=get_executors(["local_dev_testing", "large_mem"]), ignore_for_cache=["logging_file"]
)
def ic_to_wu(inputs=[], outputs=[], runtime_config={}, logging_file=None):
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


@python_app(
    cache=True, executors=get_executors(["local_dev_testing", "large_mem"]), ignore_for_cache=["logging_file"]
)
def reproject_wu(inputs=[], outputs=[], runtime_config={}, logging_file=None):
    import traceback
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.reproject_wu import reproject_wu

    logger = configure_logger("task.reproject_wu", logging_file.filepath)

    logger.info("Starting reproject_ic")
    try:
        reproject_wu(
            original_wu_filepath=inputs[0].filepath,
            uri_filepath=inputs[1].filepath,
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


@python_app(
    cache=True, executors=get_executors(["local_dev_testing", "gpu"]), ignore_for_cache=["logging_file"]
)
def kbmod_search(inputs=[], outputs=[], runtime_config={}, logging_file=None):
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


def workflow_runner(env: str = None, runtime_config: dict = {}) -> None:
    resource_config = get_resource_config(env=env)
    resource_config = apply_runtime_updates(resource_config, runtime_config)

    app_configs = runtime_config.get("apps", {})

    with parsl.load(resource_config) as dfk:
        logging_file = File(os.path.join(dfk.run_dir, "parsl.log"))
        logger = configure_logger("workflow.workflow_runner", logging_file.filepath)

        if runtime_config is not None:
            logger.info(f"Using runtime configuration definition:\n{toml.dumps(runtime_config)}")

        logger.info("Starting workflow")

        # gather all the .lst files that are staged for processing
        manifest_file = File(os.path.join(os.getcwd(), "manifest.txt"))
        create_uri_manifest_future = create_uri_manifest(
            inputs=[],
            outputs=[manifest_file],
            runtime_config=app_configs.get("create_uri_manifest", {}),
            logging_file=logging_file,
        )

        with open(create_uri_manifest_future.result(), "r") as f:
            # process each .lst file in the manifest into a .ecvs file
            uri_to_ic_futures = []
            uri_files = []
            for line in f:
                uri_file = File(line.strip())
                uri_files.append(uri_file)
                uri_to_ic_futures.append(
                    uri_to_ic(
                        inputs=[uri_file],
                        outputs=[File(line + ".ecsv")],
                        runtime_config=app_configs.get("uri_to_ic", {}),
                        logging_file=logging_file,
                    )
                )

        # create an original WorkUnit for each .ecsv file
        original_work_unit_futures = []
        for f in uri_to_ic_futures:
            original_work_unit_futures.append(
                ic_to_wu(
                    inputs=[f.result()],
                    outputs=[File(f.result().filepath + ".wu")],
                    runtime_config=app_configs.get("ic_to_wu", {}),
                    logging_file=logging_file,
                )
            )

        # reproject each WorkUnit for a range of distances
        reproject_futures = []
        for f, uri_file in zip(original_work_unit_futures, uri_files):
            for distance in range(40, 60, 10):
                reproject_futures.append(
                    reproject_wu(
                        inputs=[f.result(), uri_file],
                        outputs=[File(f.result().filepath + f".{distance}.repro")],
                        runtime_config=app_configs.get("reproject_wu", {}),
                        logging_file=logging_file,
                    )
                )

        # run kbmod search on each reprojected WorkUnit
        search_futures = []
        for f in reproject_futures:
            search_futures.append(
                kbmod_search(
                    inputs=[f.result()],
                    outputs=[File(f.result().filepath + ".search.ecsv")],
                    runtime_config=app_configs.get("kbmod_search", {}),
                    logging_file=logging_file,
                )
            )

        [f.result() for f in search_futures]

        logger.info("Workflow complete")

    parsl.clear()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        type=str,
        choices=["dev", "klone"],
        help="The environment to run the workflow in.",
    )

    parser.add_argument(
        "--runtime-config",
        type=str,
        help="The complete runtime configuration filepath to use for the workflow.",
    )

    args = parser.parse_args()

    # if a runtime_config file was provided and exists, load the toml as a dict.
    runtime_config = {}
    if args.runtime_config is not None and os.path.exists(args.runtime_config):
        with open(args.runtime_config, "r") as toml_runtime_config:
            runtime_config = toml.load(toml_runtime_config)

    workflow_runner(env=args.env, runtime_config=runtime_config)
