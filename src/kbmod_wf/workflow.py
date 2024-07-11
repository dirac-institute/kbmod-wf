import os
import parsl
from parsl import join_app, python_app, File
import parsl.executors

from kbmod_wf.utilities.configuration_utilities import get_config
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(executors=get_executors(["local_dev_testing", "local_thread"]))
def create_uri_manifest(inputs=[], outputs=[], directory_path=None, logging_file=None):
    """This app will go to a given directory, find all of the uri.lst files there,
    and copy the paths to the manifest file."""
    from kbmod_wf.utilities.logger_utilities import configure_logger

    logger = configure_logger("task.create_uri_manifest", logging_file.filepath)

    this_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.abspath(os.path.join(this_dir, "../../dev_staging"))

    # List all entries in the directory
    entries = os.listdir(directory_path)

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


@join_app
def read_and_dispatch(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger

    logger = configure_logger("task.read_and_dispatch", logging_file.filepath)

    futures = []
    with open(inputs[0].filepath, "r") as f:
        for line in f:
            future = read_and_log(
                inputs=[File(line.strip())],
                outputs=[],
                logging_file=logging_file,
            )
            futures.append(future)

    logger.info(f"Created {len(futures)} read_and_log tasks.")
    return futures


@python_app(executors=get_executors(["local_dev_testing", "small_cpu"]))
def read_and_log(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger
    import time

    time.sleep(3)
    logger = configure_logger("task.read_and_log", logging_file.filepath)

    with open(inputs[0].filepath, "r") as f:
        for line in f:
            logger.info(line.strip())

    return 1


@python_app(executors=get_executors(["local_dev_testing", "small_cpu"]))
def uri_to_ic(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.uri_to_ic import uri_to_ic

    logger = configure_logger("task.uri_to_ic", logging_file.filepath)

    logger.info("Starting uri_to_ic")
    uri_to_ic(ic_output_file=outputs[0].filepath, logger=logger)
    logger.warning("Completed uri_to_ic")


@python_app(executors=get_executors(["local_dev_testing", "small_cpu"]))
def reproject_ic(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.task_impls.reproject_ic import reproject_ic

    logger = configure_logger("task.reproject_ic", logging_file.filepath)

    logger.info("Starting reproject_ic")
    reproject_ic(logger=logger)
    logger.warning("Completed reproject_ic")


def workflow_runner(env=None):
    with parsl.load(get_config(env=env)) as dfk:
        logging_file = File(os.path.join(dfk.run_dir, "parsl.log"))

        manifest_file = File(os.path.join(os.getcwd(), "manifest.txt"))
        create_uri_manifest_future = create_uri_manifest(
            inputs=[],
            outputs=[manifest_file],
            logging_file=logging_file,
        )

        read_and_dispatch_future = read_and_dispatch(
            inputs=[create_uri_manifest_future.outputs[0]],
            outputs=[],
            logging_file=logging_file,
        )

        uri_list = File(os.path.join(os.getcwd(), "uri_list.txt"))
        uri_to_ic_future = uri_to_ic(
            inputs=[uri_list],
            outputs=[File(os.path.join(os.getcwd(), "ic.ecsv"))],
            logging_file=logging_file,
        )

        reproject_ic_future = reproject_ic(
            inputs=[uri_to_ic_future.outputs[0]],
            outputs=[],
            logging_file=logging_file,
        )

    parsl.clear()


if __name__ == "__main__":
    workflow_runner()
