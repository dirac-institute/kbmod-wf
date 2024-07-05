import os
import parsl
from datetime import datetime
from parsl import python_app, File
import parsl.executors

from kbmod_wf.configurations import *


@python_app(executors=["local_dev_testing"])
def uri_to_ic(inputs=[], outputs=[], logger=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger

    logger = configure_logger("task:uri_to_ic", inputs[-1].filepath)

    logger.info("Starting uri_to_ic")
    logger.warning("You're the cool one.")
    return 42


@python_app(executors=["local_dev_testing"])
def part2(inputs=[], outputs=[], logger=None):
    from kbmod_wf.utilities.logger_utilities import configure_logger

    logger = configure_logger("task:part2", inputs[-1].filepath)

    logger.info("Starting part2")
    return 43


def workflow_runner():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging_file = File(os.path.join(os.getcwd(), f"kbmod_wf_{timestamp}.log"))

    logger = parsl.set_file_logger(logging_file.filepath)

    with parsl.load(dev_config()):
        uri_list = File(os.path.join(os.getcwd(), "uri_list.txt"))

        uri_to_ic_future = uri_to_ic(
            inputs=[uri_list, logging_file],
            outputs=[File(os.path.join(os.getcwd(), "ic.ecsv")), logging_file],
        )

        part2_future = part2(
            inputs=[logging_file],
            outputs=[logging_file],
        )

        logger.warning("You are here")

        print(uri_to_ic_future.result())
        print(part2_future.result())

    parsl.clear()


if __name__ == "__main__":
    workflow_runner()
