from kbmod import ImageCollection
from kbmod.configuration import SearchConfiguration

import os
import glob
import time
from logging import Logger


def placeholder_ic_to_wu(ic_file=None, wu_file=None, logger=None):
    logger.info("In the ic_to_wu task_impl")
    with open(ic_file, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    with open(wu_file, "w") as f:
        f.write(f"Logged: {value} - {time.time()}\n")

    return wu_file


def ic_to_wu(
    ic_filepath: str = None, wu_filepath: str = None, runtime_config: dict = {}, logger: Logger = None
):
    """This task will convert an ImageCollection to a WorkUnit.

    Parameters
    ----------
    ic_filepath : str, optional
        The fully resolved filepath to the input ImageCollection file, by default None
    wu_filepath : str, optional
        The fully resolved filepath for the output WorkUnit file, by default None
    runtime_config : dict, optional
        Additional configuration parameters to be used at runtime, by default {}
    logger : Logger, optional
        Primary logger for the workflow, by default None

    Returns
    -------
    str
        The fully resolved filepath of the output WorkUnit file.
    """
    ic_to_wu_converter = ICtoWUConverter(
        ic_filepath=ic_filepath, wu_filepath=wu_filepath, runtime_config=runtime_config, logger=logger
    )

    return ic_to_wu_converter.create_work_unit()


class ICtoWUConverter:
    def __init__(
        self,
        ic_filepath: str = None,
        wu_filepath: str = None,
        runtime_config: dict = {},
        logger: Logger = None,
    ):
        self.ic_filepath = ic_filepath
        self.wu_filepath = wu_filepath
        self.runtime_config = runtime_config
        self.logger = logger

        self.overwrite = self.runtime_config.get("overwrite", False)
        self.search_config_filepath = self.runtime_config.get("search_config_filepath", None)

    def create_work_unit(self):
        make_wu = True
        if len(glob.glob(self.wu_filepath)):
            if self.overwrite:
                self.logger.info(f"Overwrite was {self.overwrite}. Deleting existing {self.wu_filepath}.")
                os.remove(self.wu_filepath)
            else:
                make_wu = False

        if make_wu:
            ic = ImageCollection.read(self.ic_filepath, format="ascii.ecsv")
            self.logger.info(f"ImageCollection read from {self.ic_filepath}, creating work unit next.")

            last_time = time.time()
            orig_wu = ic.toWorkUnit(config=SearchConfiguration.from_file(self.search_config_filepath))
            elapsed = round(time.time() - last_time, 1)
            self.logger.debug(f"Required {elapsed}[s] to create WorkUnit.")

            self.logger.info(f"Saving original work unit to: {self.wu_filepath}")
            last_time = time.time()
            orig_wu.to_fits(self.wu_filepath, overwrite=True)
            elapsed = round(time.time() - last_time, 1)
            self.logger.debug(f"Required {elapsed}[s] to write WorkUnit to disk: {self.wu_filepath}")

        return self.wu_filepath
