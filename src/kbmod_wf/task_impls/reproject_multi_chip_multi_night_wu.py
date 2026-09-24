import kbmod
from kbmod import ImageCollection
from kbmod.work_unit import WorkUnit


import kbmod.reprojection as reprojection
from astropy.wcs import WCS
from astropy.io import fits
from astropy.coordinates import EarthLocation
import numpy as np
import os
import time
from logging import Logger


def reproject_wu(
    guess_dist: float,
    ic_filepath: str,
    reprojected_wu_filepath: str = None,
    runtime_config: dict = {},
    logger: Logger = None,
):
    """This task will perform reflex correction and reproject a WorkUnit to a common WCS.

    Parameters
    ----------
    guess_dist: float
        The heliocentric guess distance to reproject to in AU.
    ic_filepath : str
        The fully resolved filepath to the input ImageCollection file
    reprojected_wu_filepath : str, optional
        The fully resolved filepath to the resulting WorkUnit file after reflex
        and reprojection, by default None
    runtime_config : dict, optional
        Additional configuration parameters to be used at runtime, by default {}
    logger : Logger, optional
        Primary logger for the workflow, by default None

    Returns
    -------
    str
        The fully resolved filepath of the resulting WorkUnit file after reflex
        and reprojection.
    """
    wu_reprojector = WUReprojector(
        guess_dist=guess_dist,
        ic_filepath=ic_filepath,
        reprojected_wu_filepath=reprojected_wu_filepath,
        runtime_config=runtime_config,
        logger=logger,
    )

    return wu_reprojector.reproject_workunit()


class WUReprojector:
    def __init__(
        self,
        guess_dist: float,
        ic_filepath: str = None,
        reprojected_wu_filepath: str = None,
        runtime_config: dict = {},
        logger: Logger = None,
    ):
        self.guess_dist = guess_dist
        self.ic_filepath = ic_filepath
        self.reprojected_wu_filepath = reprojected_wu_filepath
        self.runtime_config = runtime_config
        self.logger = logger
        kbmod._logging.basicConfig(level=self.logger.level)

        self.overwrite = self.runtime_config.get("overwrite", True)
        self.search_config = self.runtime_config.get("search_config", None)

        # Default to 8 workers if not in the config. Value must be 0<num workers<65.
        self.n_workers = max(1, min(self.runtime_config.get("n_workers", 8), 64))

        # If no site is configured, use the observatory from the ImageCollection.
        site = self.runtime_config.get("observation_site", None)
        self.point_on_earth = None if site is None else EarthLocation.of_site(site)

    def reproject_workunit(self):
        from kbmod_wf.task_impls.ic_to_wu import ic_to_wu

        last_time = time.time()
        self.logger.info(f"Loading a WorkUnit from ImageCollection at {self.ic_filepath}")
        wu = ic_to_wu(
            ic_filepath=self.ic_filepath,
            wu_filepath=None,
            save=False,
            runtime_config=self.runtime_config,
            logger=self.logger,
            guess_dist=self.guess_dist,
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(
            f"Required {elapsed}[s] to create original WorkUnit from ImageCollection at {self.ic_filepath}."
        )

        # The EBD fit uses the WorkUnit's observatory, so only override it when one is configured.
        if self.point_on_earth is not None:
            wu.observatory = self.point_on_earth

        # Use the global WCS that was specified from the ImageCollection.
        ic = ImageCollection.read(self.ic_filepath, format="ascii.ecsv")
        common_wcs = ic.get_global_wcs(auto_fit=False)
        if common_wcs is None:
            raise ValueError(f"No global WCS found in ImageCollection {self.ic_filepath}.")

        # Find the EBD (estimated barycentric distance) WCS for each image and reproject.
        self.logger.debug(f"Reprojecting WorkUnit with {self.n_workers} workers...")
        last_time = time.time()
        resampled_wu = reprojection.reproject_work_unit_to_distance(
            wu,
            self.guess_dist,
            common_wcs=common_wcs,
            npoints=self.runtime_config.get("ebd_fit_points", 100),
            seed=self.runtime_config.get("ebd_fit_seed", None),
            parallelize=True,
            max_parallel_processes=self.n_workers,
        )
        directory_containing_shards, wu_filename = os.path.split(self.reprojected_wu_filepath)
        resampled_wu.to_sharded_fits(wu_filename, directory_containing_shards, overwrite=self.overwrite)
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to create the sharded reprojected WorkUnit.")

        return self.reprojected_wu_filepath
