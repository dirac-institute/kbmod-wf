import kbmod
from kbmod import ImageCollection
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


def reproject_workunit_in_frame(wu, common_wcs=None, *, guess_dist=None, n_workers=8, npoints=100, seed=None):
    """Shared Rubin/DEEP WorkUnit reflex correction and image reprojection.

    ``guess_dist`` is the barycentric distance in AU. None uses original sky.
    A supplied common WCS preserves the production patch geometry. With no patch
    WCS (bounded single-chip validation), use the middle exposure in that frame.
    """
    if guess_dist is not None and not np.isfinite(guess_dist):
        raise ValueError("Distance must be finite")
    frame = "original"
    wcses = [wu.get_wcs(i) for i in range(len(wu))]
    if guess_dist is not None:
        if guess_dist <= 1.02:
            raise ValueError("EBD distance must exceed 1.02 AU")
        # The existing Rubin routine assumes homogeneous image dimensions.
        shapes = {tuple(w.array_shape) for w in wcses}
        if len(shapes) != 1:
            raise ValueError("EBD reprojection requires homogeneous input WCS shapes")
        image_height, image_width = next(iter(shapes))
        ebd, geo = transform_wcses_to_ebd(
            wcses,
            image_width,
            image_height,
            guess_dist,
            Time(wu.get_all_obstimes(), format="mjd", scale="utc"),
            wu.observatory,
            npoints=npoints,
            seed=seed,
        )
        wu.org_img_meta["ebd_wcs"] = ebd
        wu.org_img_meta["geocentric_distance"] = geo
        wu.barycentric_distance = guess_dist
        wcses = ebd
        frame = "ebd"
    if common_wcs is None:
        common_wcs = wcses[len(wcses) // 2].deepcopy()
        common_wcs.array_shape = wu.get_wcs(len(wcses) // 2).array_shape
    return reprojection.reproject_work_unit(
        wu,
        common_wcs,
        parallelize=True,
        frame=frame,
        max_parallel_processes=n_workers,
    )


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

        self.point_on_earth = EarthLocation.of_site(self.runtime_config.get("observation_site", "Rubin"))

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

        # Preserve the configured production observatory in the WorkUnit and output.
        wu.observatory = self.point_on_earth
        ic = ImageCollection.read(self.ic_filepath, format="ascii.ecsv")
        common_wcs = ic.get_global_wcs(auto_fit=False)
        if common_wcs is None:
            raise ValueError("Production ImageCollection requires an explicit patch WCS")
        last_time = time.time()
        resampled_wu = reproject_workunit_in_frame(
            wu,
            common_wcs,
            guess_dist=self.guess_dist,
            n_workers=self.n_workers,
            npoints=self.runtime_config.get("ebd_fit_points", 100),
            seed=self.runtime_config.get("ebd_fit_seed", None),
        )
        directory_containing_shards, wu_filename = os.path.split(self.reprojected_wu_filepath)
        resampled_wu.to_sharded_fits(wu_filename, directory_containing_shards, overwrite=self.overwrite)
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to create the sharded reprojected WorkUnit.")

        return self.reprojected_wu_filepath
