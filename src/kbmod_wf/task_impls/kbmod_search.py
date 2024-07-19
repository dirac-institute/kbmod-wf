import kbmod
from kbmod.work_unit import WorkUnit

import os
import time
from logging import Logger


def placeholder_kbmod_search(input_wu=None, result_file=None, logger=None):
    logger.info("In the kbmod_search task_impl")
    with open(input_wu, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    time.sleep(5)

    with open(result_file, "w") as f:
        f.write(f"Logged: {value} - {time.time()}\n")

    return result_file


def kbmod_search(
    wu_filepath: str = None,
    result_filepath: str = None,
    runtime_config: dict = {},
    logger: Logger = None,
):
    """This task will run the KBMOD search algorithm on a WorkUnit.

    Parameters
    ----------
    wu_filepath : str, optional
        The fully resolved filepath to the input WorkUnit file, by default None
    runtime_config : dict, optional
        Additional configuration parameters to be used at runtime, by default {}
    logger : Logger, optional
        Primary logger for the workflow, by default None

    Returns
    -------
    str
        The fully resolved filepath of the results file.
    """
    kbmod_searcher = KBMODSearcher(
        wu_filepath=wu_filepath,
        result_filepath=result_filepath,
        runtime_config=runtime_config,
        logger=logger,
    )

    return kbmod_searcher.run_search()


class KBMODSearcher:
    def __init__(
        self,
        wu_filepath: str = None,
        result_filepath: str = None,
        runtime_config: dict = {},
        logger: Logger = None,
    ):
        self.input_wu_filepath = wu_filepath
        self.runtime_config = runtime_config
        self.result_filepath = result_filepath
        self.logger = logger

        self.search_config_filepath = self.runtime_config.get("search_config_filepath", None)
        self.results_directory = os.path.dirname(self.result_filepath)

    def run_search(self):
        self.logger.info("Loading workunit from file")
        wu = WorkUnit.from_fits(self.input_wu_filepath)

        self.logger.debug("Loaded work unit")
        if self.search_config_filepath is not None:
            # Load a search configuration, otherwise use the one loaded with the work unit
            wu.config = kbmod.configuration.SearchConfiguration.from_file(self.search_config_filepath)

        config = wu.config

        # Modify the work unit results to be what is specified in command line args
        input_parameters = {
            "res_filepath": self.results_directory,
            "result_filename": self.result_filepath,
        }
        config.set_multiple(input_parameters)

        # Save the search config in the results directory for record keeping
        config.to_file(os.path.join(self.results_directory, "search_config.yaml"))
        wu.config = config

        self.logger.info("Running KBMOD search")
        res = kbmod.run_search.SearchRunner().run_search_from_work_unit(wu)

        self.logger.info("Search complete")
        self.logger.info(f"Number of results found: {len(res)}")

        self.logger.info(f"Writing results to output file: {self.result_filepath}")
        res.write_table(self.result_filepath)

        return self.result_filepath
