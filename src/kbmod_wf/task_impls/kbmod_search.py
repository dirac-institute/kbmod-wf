import kbmod
from kbmod.work_unit import WorkUnit

import os
import time


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
    input_wu=None, search_config_filepath=None, runtime_config={}, result_file=None, logger=None
):
    kbmod_searcher = KBMODSearcher(
        input_wu_filepath=input_wu,
        search_config_filepath=search_config_filepath,
        runtime_config=runtime_config,
        output_filepath=result_file,
        logger=logger,
    )

    return kbmod_searcher.run_search()


class KBMODSearcher:
    def __init__(self, input_wu_filepath, search_config_filepath, runtime_config, output_filepath, logger):
        self.input_wu_filepath = input_wu_filepath
        self.search_config_filepath = search_config_filepath
        self.runtime_config = runtime_config
        self.output_filepath = output_filepath
        self.logger = logger

    def run_search(self):
        self.logger.info("Loading workunit from file")
        wu = WorkUnit.from_fits(self.input_wu_filepath)

        self.logger.debug("Loaded work unit")
        if self.search_config is not None:
            # Load a search configuration, otherwise use the one loaded with the work unit
            wu.config = kbmod.configuration.SearchConfiguration.from_file(self.search_config)

        config = wu.config

        # Modify the work unit results to be what is specified in command line args
        input_parameters = {
            "res_filepath": args.result_dir,
            "result_filename": os.path.join(args.result_dir, "full_results.ecsv"),
        }
        config.set_multiple(input_parameters)

        # Save the search config in the results directory for record keeping
        config.to_file(os.path.join(args.result_dir, "search_config.yaml"))
        wu.config = config

        self.logger.info("Running KBMOD search")
        res = kbmod.run_search.SearchRunner().run_search_from_work_unit(wu)

        self.logger.info("Search complete")
        self.logger.info(f"Search results: {res}")

        self.logger.info("writing results table")
        res.write_table(self.output_filepath)

        return self.output_filepath
