from kbmod import ImageCollection
from kbmod.configuration import SearchConfiguration
from lsst.daf.butler import Butler

import os
import glob
import time
from logging import Logger


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
            ic, this_butler, self.runtime_config, self.guess_dist
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to inject objects into ImageCollection.")

        # Save injected catalog
        last_time = time.time()
        injected_cat_filepath = self.ic_filepath.replace(".collection", "_injected_catalog.ecsv")
        injected_cats.write(injected_cat_filepath, overwrite=True)
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


def ic_to_injected_ic(ic, butler, runtime_config, heliocentric_distance, n_objs_per_ic=50):
    """
    This function will take an ImageCollection and inject synthetic solar system objects into it.

    TODO update n_objs_per_ic to be a parameter in the config file

    Parameters
    ----------
    ic : ImageCollection
        The ImageCollection to inject objects into.
    butler : Butler
        The butler instance to use for the workflow.
    runtime_config : dict
        The runtime configuration for the workflow.
    heliocentric_distance : float
        The heliocentric distance of the objects to inject.
    n_objs_per_ic : int, optional
        The number of objects to inject per ImageCollection, by default 50.

    Returns
    -------
    tuple
        A tuple containing the injected ImageCollection, the injected catalogs, and the global WCS.
    """
    ## Load required configs and resources
    search_config = SearchConfiguration.from_file(
        runtime_config["apps"]["reproject_wu"]["search_config_filepath"]
    )

    # sort this here so we can rely on ordering later
    ic.data.sort("mjd_mid")

    # Procedure:
    # - get WCS
    # - get an TrajectoryGenerator
    # - extract N random vx, vy of trajectories from an trajectory sample
    #       This ensures that all our trajectories are always sampled from within the search limits
    # - use those to generate (x, y, obstime) coordinates
    # - convert those to (ra, dec, obstime) coordinates catalog
    #       This is done by using the WCS of a single image
    #       Let's say the first one.
    #       As long as a straight line in the image is approximately a straight line in the sky,
    #       i.e. as long as the image is not such a wide-field that TAN approximation does not follow great circles on the sky,
    #       the trail should be a straight line in (ra, dec)
    # - Invert correct for a parallax at a distance
    # - Go through the references in the ImageCollection and run VisitInjectTask on each
    # - store the resulting injected catalog, exposure with injected objects and mask in a list
    # - run through every standardizer and overwrite the std.exp and std.ref attributes with the new ones
    # - vstack all the individual returned injected catalogs into a big one for the WorkUnit and write it to disk
    # - create a new ImageCollection.fromStandardizers

    # Catalog generation:
    # - get wcs
    global_wcs = WCS(ic.data["global_wcs"][0])
    global_wcs.pixel_shape = (
        ic.data["global_wcs_pixel_shape_0"][0],
        ic.data["global_wcs_pixel_shape_1"][0],
    )

    # - get a sample of trajectories from TgajectoryGenerator
    eclip_angle = kbmod.wcs_utils.calc_ecliptic_angle(global_wcs)
    trjgen = kbmod.trajectory_generator.create_trajectory_generator(
        search_config["generator_config"], given_ecliptic=eclip_angle
    )
    candidates = [trj for trj in trjgen]
    trjs = np.random.choice(candidates, N_OBJS_PER_IC)

    # - generate a random sample of starting points within image bounds
    # - grab the vx, vy from the sampled trajectories
    # - figure out the pixel limits within which we're generating our starting pixels.
    #       We have 2 options here, we can get the imdiffs before-hand and query them:
    # ```maxy, maxx = imdiff.getBBox().getWidth(), imdiff.getBBox().getHeight() ```
    #       or we can use the global WCS to get the bounding box:
    pixel_boundaries = global_wcs.world_to_pixel(
        SkyCoord(global_wcs.calc_footprint()[:, 0], global_wcs.calc_footprint()[:, 1], unit="degree")
    )
    maxx = max(pixel_boundaries[0])
    maxy = max(pixel_boundaries[1])
    xs = np.random.randint(
        0, maxx, N_OBJS_PER_IC
    )  # seems ys and xs are flipped because iamge origin is in different place
    ys = np.random.randint(0, maxy, N_OBJS_PER_IC)
    vxs = np.array([t.vx for t in trjs])
    vys = np.array([t.vy for t in trjs])

    # - generate all positions of an object in the image_collection using the timestamps
    #       The np.diff gives back len()-1 array because it skips the first position.
    #       We set that diff to 0, to effectively copy the randomly sampled initial positions into
    #       the injection catalog, and that makes the N obstimes copacetic with the N dt's.
    #       A quirk of this approach is that first position is always nicely rounded pixel coordinate.
    obstimes = ic["mjd_mid"]
    obstimes.sort()
    dts = np.zeros(len(obstimes))
    dts[1:] = np.diff(obstimes)
    xs = xs[:, None] + dts * vxs[:, None]
    ys = ys[:, None] + dts * vys[:, None]
    sky_coords = global_wcs.pixel_to_world(xs, ys)

    # - invert-correct for a heliocentric distance
    sky_coords_with_distance = SkyCoord(
        sky_coords.ra, sky_coords.dec, distance=heliocentric_distance * u.au, frame="icrs"
    )
    loc = EarthLocation.of_site("Rubin")
    invert_corrected_skycoords = kbmod.reprojection_utils.invert_correct_parallax_vectorized(
        sky_coords_with_distance, obstimes, loc
    )
    invert_corrected_skycoords = sky_coords

    # Now collate a catalog for injection
    _xs, _ys, exp_id, mags, obj_ids, ts = [], [], [], [], [], []
    for (
        i,
        x,
    ) in enumerate(xs):
        # _xs.extend(x)
        obj_ids.extend(
            [
                i,
            ]
            * len(x)
        )
        mag = np.random.uniform(19, 26)
        mags.extend(
            [
                mag,
            ]
            * len(x)
        )
        ts.extend(obstimes)

    catalog = Table(
        {
            "injection_id": np.arange(0, len(obj_ids), 1),
            "ra": invert_corrected_skycoords.ra.deg.ravel(),
            "dec": invert_corrected_skycoords.dec.deg.ravel(),
            "mag": mags,
            "distance": [
                heliocentric_distance,
            ]
            * len(obj_ids),
            "source_type": [
                "Star",
            ]
            * len(obj_ids),
            "obj_ids": obj_ids,
            "obstime": ts,
            "x": xs.ravel(),
            "y": ys.ravel(),
        }
    )

    catalog = catalog.group_by("obstime")
    if len(catalog.groups) != len(ic):
        # is this really unexpected?
        raise RuntimeError("Generated catalog is missing entries for some images in collection.")

    # Injection stage
    # - set up the injection task
    # - set up references, exposures and injected_catalogs lists
    # - inject
    inject_config = VisitInjectConfig()
    inject_task = VisitInjectTask(config=inject_config)

    ## Load images to inject into
    references, exposures, injected_cats = [], [], []
    for idd, srccat in zip(ic["dataId"], catalog.groups):
        did = dafButler.DatasetId(idd)
        ref = butler.get_dataset(did, dimension_records=True)
        imdiff = butler.get(ref)
        # !!! TESTING !!!
        # imdiff.image.array = np.zeros(imdiff.image.array.shape)
        # imdiff.image.getArray()[:] = np.zeros(imdiff.image.array.shape, dtype=float)
        try:
            injected_output = inject_task.run(
                injection_catalogs=srccat,
                input_exposure=imdiff,  # .clone(), # !!! we might probably be fine not cloning this image here
                psf=imdiff.psf,
                photo_calib=imdiff.photoCalib,
                wcs=imdiff.wcs,
            )
        except RuntimeError:  # no sources were injected
            print(f"{idd} has no injected objects!")
            injected_exposure = imdiff
            injected_catalog = Table(
                {
                    "injection_id": [],
                    "ra": [],
                    "dec": [],
                    "mag": [],
                    "distance": [],
                    "source_type": [],
                    "obj_ids": [],
                    "obstime": [],
                    "x": [],
                    "y": [],
                }
            )
        else:
            injected_exposure = injected_output.output_exposure
            injected_catalog = injected_output.output_catalog
            print(f"Injected {len(injected_catalog)} objects into {idd}.")
        references.append(ref)
        exposures.append(injected_exposure)
        injected_cats.append(injected_catalog)

    # Injection post-processing stage
    # - stack the injected catalogs and write the to the correct place
    # - get standardizers and overwrite their exposures and references
    injected_cats = vstack(injected_cats)
    injected_cats.write("results_place_injected_src.cat", format="ascii.ecsv", overwrite=True)
    standardizers = ic.get_standardizers(butler=butler)
    if len(catalog.groups) != len(ic):
        raise RuntimeError(
            "Number of created standardizers does not match the number of exposures recovered after injection."
        )

    # now just overwrite the actual image arrays within each standardizer and
    # return that collection
    for std, ref, exp in zip(standardizers, references, exposures):
        std["std"].exp = exp
        std["std"].ref = ref

    # Finally make a new image collection out of these standardizers
    ic = ImageCollection.fromStandardizers([std["std"] for std in standardizers])
    return ic, injected_cats, global_wcs
