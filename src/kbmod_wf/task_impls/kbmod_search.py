import kbmod
from kbmod.work_unit import WorkUnit

import numpy as np
from logging import Logger
import os

from kbmod.filters.known_object_filters import KnownObjsMatcher
from kbmod.filters.stamp_filters import filter_stamps_by_cnn

from astropy.table import Table


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

        # Match to known objects from the results
        skybot_table_path = self.runtime_config.get("skybot_table_path", None)
        if skybot_table_path is None:
            self.logger.warning("No skybot table path provided, skipping filtering by known objects.")
        else:
            self.logger.info(f"Filtering results by known objects using table at {skybot_table_path}")
            skytable = Table.read(skybot_table_path)
                        self.logger.info(f"Read {skybot_table_path}. There are {len(skytable)} rows.")
            known_objs_matcher = KnownObjsMatcher(
                skytable,
                np.array(wu.get_all_obstimes()),
                matcher_name="known_matcher",
                sep_thresh=5.0,  # Observations must be within 5 arcsecs.
                time_thresh_s=30,  # Observations must match within 30 seconds.
                name_col="Name",
                ra_col=f"ra_{wu.barycentric_distance}",
                dec_col=f"dec_{wu.barycentric_distance}",
                mjd_col="mjd_mid",
            )

            # Carry out initial matching to known objects and populate the matches column.
            known_objs_matcher.match(res, wu.wcs)

            # Filter the matches down to results with at least 10 observations.
            min_obs = 5
            known_objs_matcher.match_on_min_obs(res, min_obs)

        # Filter results by CNN
        ml_model_path = self.runtime_config.get("ml_model_path", None)
        if ml_model_path is None:
            self.logger.warning("No ML model path provided, skipping filtering by CNN.")
        else:
            self.logger.info(f"Filtering results by CNN using model at {ml_model_path}")
            orig_res_len = len(res)
            filter_stamps_by_cnn(
                res,
                ml_model_path,
                coadd_type="mean",
            )
            res.filter_rows(res["cnn_class"])
            self.logger.info(
                f"Filtered {orig_res_len - len(res)} results using CNN model at {ml_model_path}"
            )

        self.logger.info(f"Writing results to output file: {self.result_filepath}")
        res.write_table(self.result_filepath)

        return self.result_filepath
