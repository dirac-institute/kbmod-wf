import argparse
import glob
import os
import parsl
from parsl import python_app, File
import parsl.executors

from kbmod_wf.utilities.configuration_utilities import get_resource_config
from kbmod_wf.utilities.executor_utilities import get_executors
from kbmod_wf.utilities.logger_utilities import configure_logger


@python_app(executors=get_executors(["local_dev_testing", "local_thread"]))
def create_uri_manifest(inputs=[], outputs=[], directory_path=None, logging_file=None):
    """This app will go to a given directory, find all of the uri.lst files there,
    and copy the paths to the manifest file."""
    from kbmod_wf.utilities.logger_utilities import configure_logger

    logger = configure_logger("task.create_uri_manifest", logging_file.filepath)

    if directory_path is None:
        this_dir = os.path.dirname(os.path.abspath(__file__))
        directory_path = os.path.abspath(os.path.join(this_dir, "../../dev_staging"))

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


@python_app(executors=get_executors(["local_dev_testing", "small_cpu"]))
def read_and_log(inputs=[], outputs=[], logging_file=None):
    """THIS IS A PLACEHOLDER FUNCTION THAT WILL BE REMOVED SOON"""
    from kbmod_wf.utilities.logger_utilities import configure_logger

    logger = configure_logger("task.read_and_log", logging_file.filepath)

    with open(inputs[0].filepath, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    with open(outputs[0].filepath, "w") as f:
        f.write(f"Logged: {value}")

    return outputs[0]


@python_app(executors=get_executors(["local_dev_testing", "small_cpu"]))
def uri_to_ic(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.uri_to_ic import uri_to_ic

    logger = configure_logger("task.uri_to_ic", logging_file.filepath)

    logger.info("Starting uri_to_ic")
    uri_to_ic(target_uris_file=inputs[0].filepath, ic_output_file=outputs[0].filepath, logger=logger)
    logger.warning("Completed uri_to_ic")

    return outputs[0]


@python_app(executors=get_executors(["local_dev_testing", "small_cpu"]))
def ic_to_wu(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.ic_to_wu import ic_to_wu

    logger = configure_logger("task.ic_to_wu", logging_file.filepath)

    logger.info("Starting ic_to_wu")
    ic_to_wu(ic_file=inputs[0].filepath, wu_file=outputs[0].filepath, logger=logger)
    logger.warning("Completed ic_to_wu")

    return outputs[0]


@python_app(executors=get_executors(["local_dev_testing", "small_cpu"]))
def reproject_wu(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.reproject_wu import reproject_wu

    logger = configure_logger("task.reproject_wu", logging_file.filepath)

    logger.info("Starting reproject_ic")
    reproject_wu(input_wu=inputs[0].filepath, reprojected_wu=outputs[0].filepath, logger=logger)
    logger.warning("Completed reproject_ic")

    return outputs[0]


@python_app(executors=get_executors(["local_dev_testing", "small_cpu"]))
def kbmod_search(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.kbmod_search import kbmod_search

    logger = configure_logger("task.kbmod_search", logging_file.filepath)

    logger.info("Starting kbmod_search")
    kbmod_search(input_wu=inputs[0].filepath, result_file=outputs[0].filepath, logger=logger)
    logger.warning("Completed kbmod_search")

    return outputs[0]


def workflow_runner(env=None):
    with parsl.load(get_resource_config(env=env)) as dfk:
        logging_file = File(os.path.join(dfk.run_dir, "parsl.log"))
        logger = configure_logger("workflow.workflow_runner", logging_file.filepath)

        logger.info("Starting workflow")

        # gather all the .lst files that are staged for processing
        manifest_file = File(os.path.join(os.getcwd(), "manifest.txt"))
        create_uri_manifest_future = create_uri_manifest(
            inputs=[],
            outputs=[manifest_file],
            logging_file=logging_file,
        )

        with open(create_uri_manifest_future.result(), "r") as f:
            # process each .lst file in the manifest into a .ecvs file
            uri_to_ic_futures = []
            for line in f:
                uri_to_ic_futures.append(
                    uri_to_ic(
                        inputs=[File(line.strip())],
                        outputs=[File(line + ".ecsv")],
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
                        logging_file=logging_file,
                    )
                )

            # reproject each WorkUnit for a range of distances
            reproject_futures = []
            for f in original_work_unit_futures:
                for distance in range(40, 60, 10):
                    reproject_futures.append(
                        reproject_wu(
                            inputs=[f.result()],
                            outputs=[File(f.result().filepath + f".{distance}.repro")],
                            logging_file=logging_file,
                        )
                    )

            # run kbmod search on each reprojected WorkUnit
            search_futures = []
            for f in reproject_futures:
                search_futures.append(
                    kbmod_search(
                        inputs=[f.result()],
                        outputs=[File(f.result().filepath + ".search")],
                        logging_file=logging_file,
                    )
                )

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

    args = parser.parse_args()

    workflow_runner(env=args.env)
