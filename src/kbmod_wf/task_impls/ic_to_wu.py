from kbmod import ImageCollection
from kbmod.configuration import SearchConfiguration
from lsst.daf.butler import Butler

import os
import glob
import time
from logging import Logger


def ic_to_wu(
    ic_filepath: str = None, wu_filepath: str = None, save: bool = True, runtime_config: dict = {}, logger: Logger = None
):
    """This task will convert an ImageCollection to a WorkUnit.

    Parameters
    ----------
    ic_filepath : str, optional
        The fully resolved filepath to the input ImageCollection file, by default None
    wu_filepath : str, optional
        The fully resolved filepath for the output WorkUnit file, by default None
    save : bool, optional
        Flag to save the WorkUnit to disk, by default True. If False, the WorkUnit is returned.
    runtime_config : dict, optional
        Additional configuration parameters to be used at runtime, by default {}
    logger : Logger, optional
        Primary logger for the workflow, by default None

    Returns
    -------
    str | WorkUnit
        The fully resolved filepath of the output WorkUnit file or the WorkUnit object itself if save=False.
    """
    ic_to_wu_converter = ICtoWUConverter(
        ic_filepath=ic_filepath, wu_filepath=wu_filepath, save=save, runtime_config=runtime_config, logger=logger
    )

    return ic_to_wu_converter.create_work_unit()


class ICtoWUConverter:
    def __init__(
        self,
        ic_filepath: str = None,
        wu_filepath: str = None,
        save: bool = True,
        runtime_config: dict = {},
        logger: Logger = None,
    ):
        self.ic_filepath = ic_filepath
        self.wu_filepath = wu_filepath
        self.save = save
        self.runtime_config = runtime_config
        self.logger = logger

        self.search_config_filepath = self.runtime_config.get("search_config_filepath", None)

    def create_work_unit(self):
        ic = ImageCollection.read(self.ic_filepath, format="ascii.ecsv")
        self.logger.info(f"ImageCollection read from {self.ic_filepath}, creating work unit next.")

        last_time = time.time()
        self.logger.info("Creating butler instance")
        this_butler = Butler(self.runtime_config.get("butler_config_filepath", None))
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to instantiate butler.")

        last_time = time.time()
        orig_wu = ic.toWorkUnit(
            search_config=SearchConfiguration.from_file(self.search_config_filepath), butler=this_butler
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to create WorkUnit.")

        if not self.save:
            self.logger.debug(f"Required {elapsed}[s] to create the WorkUnit.")
            return orig_wu

        self.logger.info(f"Saving sharded work unit to: {self.wu_filepath}")
        last_time = time.time()
        directory_containing_shards, wu_filename = os.path.split(self.wu_filepath)
        orig_wu.to_sharded_fits(wu_filename, directory_containing_shards, overwrite=True)
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to write WorkUnit to disk: {self.wu_filepath}")

        return self.wu_filepath
