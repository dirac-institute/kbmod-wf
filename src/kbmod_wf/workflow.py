import os
import parsl
from parsl import python_app, File
import parsl.executors

from kbmod_wf.configurations import *


@python_app(executors=["local_dev_testing"])
def uri_to_ic(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.tasks.uri_to_ic import uri_to_ic

    logger = configure_logger("task.uri_to_ic", logging_file.filepath)

    logger.info("Starting uri_to_ic")
    output = uri_to_ic(logger=logger)
    logger.warning("You're the cool one.")
    return output


@python_app(executors=["local_dev_testing"])
def reproject_ic(inputs=[], outputs=[], logging_file=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger
    from kbmod_wf.tasks.reproject_ic import reproject_ic

    logger = configure_logger("task.reproject_ic", logging_file.filepath)

    logger.info("Starting reproject_ic")
    output = reproject_ic(logger=logger)
    logger.warning("This is a slow step.")
    return output


def workflow_runner():
    with parsl.load(dev_config()) as dfk:
        logging_file = File(os.path.join(dfk.run_dir, "parsl.log"))

        uri_list = File(os.path.join(os.getcwd(), "uri_list.txt"))
        uri_to_ic_future = uri_to_ic(
            inputs=[uri_list],
            outputs=[File(os.path.join(os.getcwd(), "ic.ecsv"))],
            logging_file=logging_file,
        )

        reproject_ic_future = reproject_ic(
            inputs=[],
            outputs=[],
            logging_file=logging_file,
        )

        print(uri_to_ic_future.result())
        print(reproject_ic_future.result())

    parsl.clear()


if __name__ == "__main__":
    workflow_runner()
