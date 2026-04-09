import os
import time
from logging import Logger

import pandas as pd
from astropy.table import Table

from kbmod import ImageCollection
from kbmod.configuration import SearchConfiguration
from lsst.daf.butler import Butler


def ic_to_wu(
    ic_filepath: str = None,
    wu_filepath: str = None,
    save: bool = True,
    runtime_config: dict = {},
    logger: Logger = None,
    guess_dist: float = None,
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
    guess_dist : float, optional
        The guess distance for the WorkUnit, by default None

    Returns
    -------
    str | WorkUnit
        The fully resolved filepath of the output WorkUnit file or the WorkUnit object itself if save=False.
    """
    ic_to_wu_converter = ICtoWUConverter(
        ic_filepath=ic_filepath,
        wu_filepath=wu_filepath,
        save=save,
        runtime_config=runtime_config,
        logger=logger,
        guess_dist=guess_dist,
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
        guess_dist: float = None,
    ):
        self.ic_filepath = ic_filepath
        self.wu_filepath = wu_filepath
        self.save = save
        self.runtime_config = runtime_config
        self.logger = logger
        self.guess_dist = guess_dist

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
        ic, injected_cats, global_wcs = ic_to_injected_ic(
            ic,
            this_butler,
            self.runtime_config,
            self.guess_dist,
            ic_filepath=self.ic_filepath,
            logger=self.logger,
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to inject objects into ImageCollection.")

        # Save injected catalog as parquet alongside the input ImageCollection
        last_time = time.time()
        injected_cat_filepath = str(self.ic_filepath) + ".injection_cat.parquet"
        injected_cats.to_pandas().to_parquet(injected_cat_filepath)
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to save injected catalog to: {injected_cat_filepath}")

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


def ic_to_injected_ic(ic, butler, runtime_config, heliocentric_distance, ic_filepath=None, logger=None):
    """
    Inject synthetic solar system objects into an ImageCollection.

    Supports two modes:
    1. **Generative**: Randomly generate an injection catalog using `ic.generate_injection_catalog()`
    2. **Pre-Computed**: Load a pre-existing catalog from a mapping file

    Parameters
    ----------
    ic : ImageCollection
        The ImageCollection to inject objects into.
    butler : Butler
        The butler instance to use for the workflow.
    runtime_config : dict
        The runtime configuration for the workflow. May contain an "injection" subsection with:
        - n_objs_per_ic : int - Number of objects to inject (default: 50)
        - mag_range : list - [min, max] magnitude range (default: [19.0, 26.0])
        - catalog_mapping_path : str - Path to parquet file mapping IC paths to catalog paths
    heliocentric_distance : float
        The heliocentric distance (AU) of the objects to inject.
    ic_filepath : str, optional
        The filepath of the input ImageCollection, used for catalog mapping lookup.
    logger : Logger, optional
        Logger for debug output.

    Returns
    -------
    tuple
        A tuple containing (injected_ic, injected_cats, global_wcs).

    Raises
    ------
    RuntimeError
        If the standardizer does not support the "INJECTED" mask plane.
    ValueError
        If catalog_mapping_path is specified but no matching catalog is found.
    """
    # Get injection-specific config (may be nested under "injection" key)
    injection_config = runtime_config.get("injection", {})

    # Extract config parameters with defaults
    n_objs_per_ic = injection_config.get("n_objs_per_ic", 50)
    mag_range = injection_config.get("mag_range", [19.0, 26.0])
    catalog_mapping_path = injection_config.get("catalog_mapping_path", None)

    if logger:
        logger.debug(f"Injection config: n_objs={n_objs_per_ic}, mag_range={mag_range}")

    # Safety validation: Check for INJECTED bit flag support
    _validate_injected_mask_support(ic, butler, logger)

    # Determine catalog source: pre-computed or generative
    catalog = None

    if catalog_mapping_path is not None:
        # Pre-computed catalog mode: look up catalog path from mapping file
        catalog = _load_catalog_from_mapping(catalog_mapping_path, ic_filepath, logger)

    if catalog is None:
        # Generative mode: create catalog using kbmod's injection module
        if logger:
            logger.info(f"Generating injection catalog with {n_objs_per_ic} objects")

        search_config = SearchConfiguration.from_file(runtime_config.get("search_config_filepath", None))

        catalog = ic.generate_injection_catalog(
            search_config=search_config,
            global_wcs=ic.get_global_wcs(auto_fit=False),
            n_objs_per_ic=n_objs_per_ic,
            guess_distance=heliocentric_distance,
            mag_range=tuple(mag_range),
        )

    # Perform the injection
    injected_ic, injected_cats = ic.inject_sources(catalog=catalog, butler=butler)

    return injected_ic, injected_cats, None


def _validate_injected_mask_support(ic, butler, logger=None):
    """
    Validate that the standardizer supports the "INJECTED" mask plane.

    This checks that the underlying exposure's mask plane dictionary contains
    the "INJECTED" bit flag, which is required for proper source injection tracking.

    Parameters
    ----------
    ic : ImageCollection
        The ImageCollection to validate.
    butler : Butler
        The butler instance to retrieve exposures.
    logger : Logger, optional
        Logger for debug output.

    Raises
    ------
    RuntimeError
        If the "INJECTED" mask plane is not supported.
    """
    try:
        standardizers = ic.get_standardizers(butler=butler)
        if not standardizers:
            if logger:
                logger.warning("No standardizers found, skipping INJECTED mask validation")
            return

        # Check the first standardizer's mask plane dict
        std = standardizers[0]["std"]

        # The standardizer should have an exposure with a mask
        if hasattr(std, "exp") and std.exp is not None:
            mask_plane_dict = std.exp.mask.getMaskPlaneDict()
            if "INJECTED" not in mask_plane_dict:
                raise RuntimeError(
                    "The standardizer's exposure does not support the 'INJECTED' mask plane. "
                    "Source injection tracking requires this mask plane to be defined. "
                    f"Available mask planes: {list(mask_plane_dict.keys())}"
                )
            if logger:
                logger.debug("INJECTED mask plane validation passed")
        else:
            if logger:
                logger.warning("Standardizer has no exposure loaded, skipping INJECTED mask validation")

    except Exception as e:
        if "INJECTED" in str(e):
            raise
        if logger:
            logger.warning(f"Could not validate INJECTED mask support: {e}")


def _load_catalog_from_mapping(mapping_path, ic_filepath, logger=None):
    """
    Load a pre-computed injection catalog from a mapping file.

    The mapping file is a parquet table with columns:
    - ic_filepath: The path to the ImageCollection file
    - catalog_filepath: The path to the corresponding injection catalog

    Parameters
    ----------
    mapping_path : str
        Path to the parquet mapping file.
    ic_filepath : str
        The filepath of the current ImageCollection to look up.
    logger : Logger, optional
        Logger for debug output.

    Returns
    -------
    astropy.table.Table or None
        The loaded catalog, or None if no mapping was found.
    """
    if not os.path.exists(mapping_path):
        if logger:
            logger.warning(f"Catalog mapping file not found: {mapping_path}")
        return None

    if logger:
        logger.info(f"Loading catalog mapping from: {mapping_path}")

    mapping_df = pd.read_parquet(mapping_path)

    # Normalize the IC filepath for comparison
    ic_filepath_normalized = os.path.abspath(ic_filepath) if ic_filepath else None

    # Look up the catalog path
    if "ic_filepath" not in mapping_df.columns or "catalog_filepath" not in mapping_df.columns:
        if logger:
            logger.warning(
                f"Mapping file missing required columns. Expected 'ic_filepath' and 'catalog_filepath', "
                f"got: {list(mapping_df.columns)}"
            )
        return None

    # Try exact match first, then normalized paths
    match = mapping_df[mapping_df["ic_filepath"] == ic_filepath]
    if len(match) == 0 and ic_filepath_normalized:
        # Try with normalized paths
        mapping_df["ic_filepath_norm"] = mapping_df["ic_filepath"].apply(
            lambda x: os.path.abspath(x) if x else None
        )
        match = mapping_df[mapping_df["ic_filepath_norm"] == ic_filepath_normalized]

    if len(match) == 0:
        if logger:
            logger.info(f"No pre-computed catalog found for IC: {ic_filepath}")
        return None

    catalog_path = match.iloc[0]["catalog_filepath"]
    if logger:
        logger.info(f"Found pre-computed catalog: {catalog_path}")

    # Load the catalog
    if not os.path.exists(catalog_path):
        if logger:
            logger.warning(f"Mapped catalog file not found: {catalog_path}")
        return None

    if catalog_path.endswith(".parquet"):
        catalog = Table.from_pandas(pd.read_parquet(catalog_path))
    elif catalog_path.endswith(".ecsv"):
        catalog = Table.read(catalog_path, format="ascii.ecsv")
    else:
        catalog = Table.read(catalog_path)

    if logger:
        logger.info(f"Loaded pre-computed catalog with {len(catalog)} rows")

    return catalog
