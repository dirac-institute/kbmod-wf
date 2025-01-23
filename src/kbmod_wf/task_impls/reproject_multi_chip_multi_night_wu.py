import kbmod
from kbmod import ImageCollection
from kbmod.work_unit import WorkUnit
from kbmod.reprojection_utils import transform_wcses_to_ebd


import kbmod.reprojection as reprojection
from kbmod_wf.task_impls.ic_to_wu import ic_to_wu
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

        self.overwrite = self.runtime_config.get("overwrite", False)
        self.search_config = self.runtime_config.get("search_config", None)

        # Default to 8 workers if not in the config. Value must be 0<num workers<65.
        self.n_workers = max(1, min(self.runtime_config.get("n_workers", 8), 64))

        self.point_on_earth = EarthLocation.of_site(self.runtime_config.get("observation_site", "ctio"))

    def reproject_workunit(self):
        last_time = time.time()
        self.logger.info(f"Loading a WorkUnit from ImageCollection at {self.ic_filepath}")
        wu = ic_to_wu(
            ic_filepath=self.ic_filepath,
            wu_filepath=None,
            save=False,
            runtime_config=self.runtime_config,
            logger=self.logger,
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(
            f"Required {elapsed}[s] to create original WorkUnit from ImageCollection at {self.ic_filepath}."
        )

        #! This method to get image dimensions won't hold if the images are different sizes.
        image_height, image_width = wu.get_wcs(0).array_shape

        # Find the EBD (estimated barycentric distance) WCS for each image
        last_time = time.time()
        ebd_per_image_wcs, geocentric_dists = transform_wcses_to_ebd(
            [wu.get_wcs(i) for i in range(len(wu))],
            image_width,
            image_height,
            self.guess_dist,  # heliocentric guess distance in AU
            Time(wu.get_all_obstimes(), format="mjd"),
            self.point_on_earth,
            npoints=10,
            seed=None,
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to transform WCS objects to EBD..")

        wu.org_img_meta["ebd_wcs"] = ebd_per_image_wcs
        wu.heliocentric_distance = self.guess_dist
        wu.org_img_meta["geocentric_distance"] = geocentric_dists

        # Reproject to a common WCS using the WCS for our patch
        self.logger.debug(f"Reprojecting WorkUnit with {self.n_workers} workers...")
        last_time = time.time()

        # Use the global WCS that was specified from the ImageCollection.
        ic = ImageCollection.read(self.ic_filepath, format="ascii.ecsv")

        # Pick the first global WCS and pixel shape from the ImageCollection
        common_wcs = WCS(ic.data["global_wcs"][0])
        common_wcs.pixel_shape = (
            ic.data["global_wcs_pixel_shape_0"][0],
            ic.data["global_wcs_pixel_shape_1"][0],
        )

        resampled_wu = reprojection.reproject_work_unit(
            wu,
            common_wcs,
            parallelize=True,
            frame="ebd",
            max_parallel_processes=self.n_workers,
        )
        directory_containing_shards, wu_filename = os.path.split(self.reprojected_wu_filepath)
        resampled_wu.to_sharded_fits(wu_filename, directory_containing_shards, overwrite=self.overwrite)
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to create the sharded reprojected WorkUnit.")

        return self.reprojected_wu_filepath
