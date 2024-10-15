import kbmod
from kbmod.work_unit import WorkUnit
from kbmod.reprojection_utils import transform_wcses_to_ebd

import kbmod.reprojection as reprojection
from astropy.wcs import WCS
from astropy.io import fits
from astropy.coordinates import EarthLocation
from astropy.time import Time
import numpy as np
import os
import time
from logging import Logger


def reproject_wu(
    guess_dist: float,
    original_wu_filepath: str = None,
    reprojected_wu_filepath: str = None,
    runtime_config: dict = {},
    logger: Logger = None,
):
    """This task will perform reflex correction and reproject a WorkUnit to a common WCS.

    Parameters
    ----------
    guess_dist: float
        The heliocentric guess distance to reproject to in AU.
    original_wu_filepath : str, optional
        The fully resolved filepath to the input WorkUnit file, by default None
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
        original_wu_filepath=original_wu_filepath,
        reprojected_wu_filepath=reprojected_wu_filepath,
        runtime_config=runtime_config,
        logger=logger,
    )

    return wu_reprojector.reproject_workunit()


class WUReprojector:
    def __init__(
        self,
        guess_dist: float,
        original_wu_filepath: str = None,
        reprojected_wu_filepath: str = None,
        runtime_config: dict = {},
        logger: Logger = None,
    ):
        self.guess_dist = guess_dist
        self.original_wu_filepath = original_wu_filepath
        self.reprojected_wu_filepath = reprojected_wu_filepath
        self.runtime_config = runtime_config
        self.logger = logger
        kbmod._logging.basicConfig(level=self.logger.level)

        self.overwrite = self.runtime_config.get("overwrite", False)
        self.search_config = self.runtime_config.get("search_config", None)

        # Default to 8 workers if not in the config. Value must be 0<num workers<65.
        self.n_workers = max(1, min(self.runtime_config.get("n_workers", 8), 64))

        self.point_on_earth = EarthLocation.of_site(self.runtime_config.get("observation_site", "ctio"))

    def reproject_workunit(self):
        last_time = time.time()
        self.logger.info(f"Lazy reading existing WorkUnit from disk: {self.original_wu_filepath}")
        directory_containing_shards, wu_filename = os.path.split(self.original_wu_filepath)
        wu = WorkUnit.from_sharded_fits(wu_filename, directory_containing_shards, lazy=True)
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(
            f"Required {elapsed}[s] to lazy read original WorkUnit {self.original_wu_filepath}."
        )

        #! This method to get image dimensions won't hold if the images are different sizes.
        image_height, image_width = wu._per_image_wcs[0].array_shape

        # Find the EBD (estimated barycentric distance) WCS for each image
        last_time = time.time()
        ebd_per_image_wcs, geocentric_dists = transform_wcses_to_ebd(
            wu._per_image_wcs,
            image_width,
            image_height,
            self.guess_dist, # heliocentric guess distance in AU
            Time(wu.get_all_obstimes(), format="mjd"),
            self.point_on_earth,
            npoints=10,
            seed=None,
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to transform WCS objects to EBD..")

        if len(wu._per_image_wcs) != len(ebd_per_image_wcs):
            raise ValueError(
                f"Number of barycentric WCS objects ({len(ebd_per_image_wcs)}) does not match the original number of images ({len(wu._per_image_wcs)})."
            )

        wu._per_image_ebd_wcs = ebd_per_image_wcs
        wu.heliocentric_distance = self.guess_dist
        wu.geocentric_distances = geocentric_dists

        # Reproject to a common WCS using the WCS for our patch
        self.logger.debug(f"Reprojecting WorkUnit with {self.n_workers} workers...")
        last_time = time.time()

        directory_containing_reprojected_shards, reprojected_wu_filename = os.path.split(
            self.reprojected_wu_filepath
        )
        reprojection.reproject_lazy_work_unit(
            wu,
            wu.wcs, # Use the common WCS of the WorkUnit
            directory_containing_reprojected_shards,
            reprojected_wu_filename,
            frame="ebd",
            max_parallel_processes=self.n_workers,
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to create the sharded reprojected WorkUnit.")

        return self.reprojected_wu_filepath
