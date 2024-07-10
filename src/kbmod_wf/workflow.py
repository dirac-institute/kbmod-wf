import os
import parsl
from parsl import python_app, File
import parsl.executors

from kbmod_wf.utilities.configuration_utilities import get_config
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(executors=get_executors(["local_dev_testing", "local_thread"]))
def create_uri_manifest(inputs=[], outputs=[], directory_path=None, logging_file=None):
    """This app will go to a given directory, find all of the uri.lst files there,
    and copy the paths to the manifest file."""
    from kbmod_wf.utilities.logger_utilities import configure_logger

    logger = configure_logger("task.create_uri_manifest", logging_file.filepath)

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
