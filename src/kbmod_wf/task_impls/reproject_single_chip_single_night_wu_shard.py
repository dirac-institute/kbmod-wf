import kbmod
from kbmod.work_unit import WorkUnit

import kbmod.reprojection as reprojection

from reproject.mosaicking import find_optimal_celestial_wcs
import os
import time
from logging import Logger


def reproject_wu_shard(
    original_wu_shard_filepath: str = None,
    reprojected_wu_shard_filepath: str = None,
    runtime_config: dict = {},
    logger: Logger = None,
):
    """This task will reproject a WorkUnit to a common WCS.

    Parameters
    ----------
    original_wu_shard_filepath : str, optional
        The fully resolved filepath to the input WorkUnit file, by default None
    reprojected_wu_shard_filepath : str, optional
        The fully resolved filepath to the resulting WorkUnit file after
        reprojection, by default None
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
    wu_shard_reprojector = WUShardReprojector(
        original_wu_filepath=original_wu_shard_filepath,
        reprojected_wu_filepath=reprojected_wu_shard_filepath,
        runtime_config=runtime_config,
        logger=logger,
    )

    return wu_shard_reprojector.reproject_workunit()


class WUShardReprojector:
    def __init__(
        self,
        original_wu_filepath: str = None,
        reprojected_wu_filepath: str = None,
        runtime_config: dict = {},
        logger: Logger = None,
    ):
        self.original_wu_filepath = original_wu_filepath
        self.reprojected_wu_filepath = reprojected_wu_filepath
        self.runtime_config = runtime_config
        self.logger = logger

        # Default to 8 workers if not in the config. Value must be 0<num workers<65.
        self.n_workers = max(1, min(self.runtime_config.get("n_workers", 8), 64))

    def reproject_workunit_shard(self):
        last_time = time.time()
        self.logger.info(f"Lazy reading existing WorkUnit from disk: {self.original_wu_filepath}")
        directory_containing_shards, wu_filename = os.path.split(self.original_wu_filepath)
        wu = WorkUnit.from_sharded_fits(wu_filename, directory_containing_shards, lazy=True)
        elapsed = round(time.time() - last_time, 1)
        self.logger.info(f"Required {elapsed}[s] to lazy read original WorkUnit {self.original_wu_filepath}.")

        directory_containing_reprojected_shards, reprojected_wu_filename = os.path.split(
            self.reprojected_wu_filepath
        )

        # Reproject to a common WCS using the WCS for our patch
        self.logger.info(f"Reprojecting WorkUnit with {self.n_workers} workers...")
        last_time = time.time()

        opt_wcs, shape = find_optimal_celestial_wcs(list(wu._per_image_wcs))
        opt_wcs.array_shape = shape
        reprojection.reproject_work_unit(
            wu,
            opt_wcs,
            max_parallel_processes=self.n_workers,
            write_output=True,
            directory=directory_containing_reprojected_shards,
            filename=reprojected_wu_filename,
        )

        elapsed = round(time.time() - last_time, 1)
        self.logger.info(f"Required {elapsed}[s] to create the sharded reprojected WorkUnit.")

        return self.reprojected_wu_filepath
