from kbmod import ImageCollection
from kbmod.configuration import SearchConfiguration

import os
import glob
import time


def placeholder_ic_to_wu(ic_file=None, wu_file=None, logger=None):
    logger.info("In the ic_to_wu task_impl")
    with open(ic_file, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    with open(wu_file, "w") as f:
        f.write(f"Logged: {value} - {time.time()}\n")

    return wu_file


def ic_to_wu(ic_file=None, runtime_config={}, wu_file=None, logger=None):
    ic_to_wu_converter = ICtoWUConverter(
        ic_file=ic_file, runtime_config=runtime_config, wu_file=wu_file, logger=logger
    )

    return ic_to_wu_converter.create_work_unit()


class ICtoWUConverter:
    def __init__(self, ic_file=None, runtime_config={}, wu_file=None, logger=None):
        self.ic_file = ic_file
        self.runtime_config = runtime_config
        self.wu_file = wu_file
        self.logger = logger

        self.overwrite = self.runtime_config.get("overwrite", False)
        self.search_config = self.runtime_config.get("search_config", None)

    def create_work_unit(self):
        make_wu = True
        if len(glob.glob(self.wu_file)):
            if self.overwrite:
                self.logger.debug(f"Overwrite was {self.overwrite}. Deleting existing {self.wu_file}.")
                os.remove(self.wu_file)
            else:
                make_wu = False

        if make_wu:
            ic = ImageCollection.read(self.ic_input_file, format="ascii.ecsv")
            self.logger.debug(f"ImageCollection read from {self.ic_input_file}, creating work unit next.")

            last_time = time.time()
            orig_wu = ic.toWorkUnit(config=SearchConfiguration.from_file(self.search_config))
            elapsed = round(time.time() - last_time, 1)
            self.logger.info(f"{elapsed} seconds to create WorkUnit.")

            last_time = time.time()
            orig_wu.to_fits(self.wu_file, overwrite=True)
            elapsed = round(time.time() - last_time, 1)
            self.logger.info(f"Saving original work unit to: {self.wu_file}")
            self.logger.info(f"{elapsed} seconds to write WorkUnit to disk: {self.wu_file}")

        return self.wu_file
