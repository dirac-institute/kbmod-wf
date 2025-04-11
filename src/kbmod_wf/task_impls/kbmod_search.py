import kbmod
from kbmod.work_unit import WorkUnit

import os
from logging import Logger


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
        self.cleanup_wu = self.runtime_config.get("cleanup_wu", False)
        self.results_directory = os.path.dirname(self.result_filepath)

    def run_search(self):
        # Check that KBMOD has access to a GPU before starting the search.
        if not kbmod.search.HAS_GPU:
            raise RuntimeError("Code compiled without GPU support.")
        else:
            self.logger.info("Confirmed GPU avaliable.")

        self.logger.info("Loading workunit from file")
        directory_containing_shards, wu_filename = os.path.split(self.input_wu_filepath)
        wu = WorkUnit.from_sharded_fits(wu_filename, directory_containing_shards, lazy=False)
        self.logger.debug("Loaded work unit")

        #! Seems odd that we extract, modify, and reset the config in the workunit.
        #! Can we just modify the config in the workunit directly?
        if self.search_config_filepath is not None:
            # Load a search configuration, otherwise use the one loaded with the work unit
            wu.config = kbmod.configuration.SearchConfiguration.from_file(self.search_config_filepath)

        config = wu.config

        # Modify the work unit results to be what is specified in command line args
        base_filename, _ = os.path.splitext(os.path.basename(self.result_filepath))
        input_parameters = {
            "res_filepath": self.results_directory,
            "result_filename": self.result_filepath,
            "output_suffix": base_filename,
        }
        config.set_multiple(input_parameters)

        wu.config = config

        self.logger.info("Running KBMOD search")
        res = kbmod.run_search.SearchRunner().run_search_from_work_unit(wu)
        self.logger.info("Search complete")
        self.logger.info(f"Number of results found: {len(res)}")

        self.logger.info(f"Writing results to output file: {self.result_filepath}")
        res.write_table(self.result_filepath)
        self.logger.info("Results written to file")
    
        if self.cleanup_wu:
            self.logger.info(f"Cleaning up sharded WorkUnit {self.input_wu_filepath} with {len(wu)}")
            # Delete the head filefor the WorkUnit
            try:
                os.remove(self.input_wu_filepath)
            except Exception as e:
                self.logger.warning(f"Failed to remove {self.input_wu_filepath}: {e}")

            # Delete the individual shards for this WorkUnit, one existing for each image.
            for i in range(len(wu)):
                shard_path = os.path.join(directory_containing_shards, f"{i}_{wu_filename}")
                try:
                    os.remove(shard_path)
                except Exception as e:
                    self.logger.warning(f"Failed to remove WorkUnit shard {shard_path}: {e}")
            self.logger.info(f"Successfully removed WorkUnit {self.input_wu_filepath}")

        return self.result_filepath
