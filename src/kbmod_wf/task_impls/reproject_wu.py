from kbmod.work_unit import WorkUnit
from kbmod.reprojection_utils import transform_wcses_to_ebd

import kbmod.reprojection as reprojection
from astropy.wcs import WCS
from astropy.io import fits
from astropy.coordinates import EarthLocation
import astropy.time
import numpy as np
import os
import time
from logging import Logger


def placeholder_reproject_wu(input_wu=None, reprojected_wu=None, logger=None):
    logger.info("In the reproject_wu task_impl")
    with open(input_wu, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    with open(reprojected_wu, "w") as f:
        f.write(f"Logged: {value} - {time.time()}\n")

    return reprojected_wu


def reproject_wu(
    original_wu_filepath: str = None,
    uri_filepath: str = None,
    reprojected_wu_filepath: str = None,
    runtime_config: dict = {},
    logger: Logger = None,
):
    """This task will perform reflex correction and reproject a WorkUnit to a common WCS.

    Parameters
    ----------
    original_wu_filepath : str, optional
        The fully resolved filepath to the input WorkUnit file, by default None
    uri_filepath : str, optional
        The fully resolved filepath to the original uri file. This is used
        exclusively for the header contents, by default None
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
        original_wu_filepath=original_wu_filepath,
        uri_filepath=uri_filepath,
        reprojected_wu_filepath=reprojected_wu_filepath,
        runtime_config=runtime_config,
        logger=logger,
    )

    return wu_reprojector.reproject_workunit()


class WUReprojector:
    def __init__(
        self,
        original_wu_filepath: str = None,
        uri_filepath: str = None,
        reprojected_wu_filepath: str = None,
        runtime_config: dict = {},
        logger: Logger = None,
    ):
        self.original_wu_filepath = original_wu_filepath
        self.uri_filepath = uri_filepath
        self.reprojected_wu_filepath = reprojected_wu_filepath
        self.runtime_config = runtime_config
        self.logger = logger

        self.overwrite = self.runtime_config.get("overwrite", False)
        self.search_config = self.runtime_config.get("search_config", None)

        # Default to 8 workers if not in the config. Value must be 0<num workers<65.
        self.n_workers = max(1, min(self.runtime_config.get("n_workers", 8), 64))

        self.uri_params = self._get_params_from_uri_file(uri_file=self.uri_filepath)
        self.patch_size = self.uri_params["patch_size"]
        self.pixel_scale = self.uri_params["pixel_scale"]
        self.guess_dist = self.uri_params["dist_au"]  # ! Let's update the terminology here to be consistent.
        self.patch_corners = self.uri_params[
            "patch_box"
        ]  # ! Let's update the terminology here to be consistent.

        # handle image dimensions
        if self.image_width == None or self.image_height == None:
            if "patch_size" not in self.uri_params:
                raise KeyError(
                    f"Must supply image dimensions (image_width, image_height) or #patch_size= must be in a specified URI file."
                )
            if self.pixel_scale == None:
                raise KeyError(
                    f"When patch pixel dimensions are not specifified, the user must supply a pixel scale via the command line or the uri file."
                )

        self.image_width, self.image_height = self._patch_arcmin_to_pixels(
            patch_size_arcmin=self.patch_size,
            pixel_scale_arcsec_per_pix=self.pixel_scale,
        )

        self.point_on_earth = EarthLocation.of_site(self.runtime_config.get("observation_site", "ctio"))

    def reproject_workunit(self):
        # Create a WCS object for the patch. This will be our common reprojection WCS
        self.logger.debug(f"Creating WCS from patch")
        patch_wcs = self._create_wcs_from_corners(
            self.patch_corners,
            self.image_width,
            self.image_height,
            pixel_scale=self.pixel_scale,
        )

        self.logger.info(f"Reading existing WorkUnit from disk: {self.original_wu_filepath}")
        orig_wu = WorkUnit.from_fits(self.original_wu_filepath)
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to read original WorkUnit {self.original_wu_filepath}.")

        # gather elements needed for reproject phase
        imgs = orig_wu.im_stack

        # Find the EBD (estimated barycentric distance) WCS for each image
        last_time = time.time()
        ebd_per_image_wcs, geocentric_dists = transform_wcses_to_ebd(
            orig_wu._per_image_wcs,
            imgs.get_single_image(0).get_width(),
            imgs.get_single_image(0).get_height(),
            self.guess_dist,
            [astropy.time.Time(img.get_obstime(), format="mjd") for img in imgs.get_images()],
            self.point_on_earth,
            npoints=10,
            seed=None,
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to transform WCS objects to EBD..")

        if len(orig_wu._per_image_wcs) != len(ebd_per_image_wcs):
            raise ValueError(
                f"Number of barycentric WCS objects ({len(ebd_per_image_wcs)}) does not match the original number of images ({len(orig_wu._per_image_wcs)})."
            )

        # Construct a WorkUnit with the EBD WCS and provenance data
        self.logger.debug(f"Creating Barycentric WorkUnit...")
        last_time = time.time()
        ebd_wu = WorkUnit(
            im_stack=orig_wu.im_stack,
            config=orig_wu.config,
            per_image_wcs=orig_wu._per_image_wcs,
            per_image_ebd_wcs=ebd_per_image_wcs,
            heliocentric_distance=self.guess_dist,
            geocentric_distances=geocentric_dists,
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to create EBD WorkUnit.")

        # Reproject to a common WCS using the WCS for our patch
        self.logger.debug(f"Reprojecting WorkUnit with {self.n_workers} workers...")
        last_time = time.time()
        reprojected_wu = reprojection.reproject_work_unit(
            ebd_wu, patch_wcs, frame="ebd", max_parallel_processes=self.n_workers
        )
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(f"Required {elapsed}[s] to create the reprojected WorkUnit.")

        # Save the reprojected WorkUnit
        self.logger.debug(f"Saving reprojected work unit to: {self.reprojected_wu_filepath}")
        last_time = time.time()
        reprojected_wu.to_fits(self.reprojected_wu_filepath)
        elapsed = round(time.time() - last_time, 1)
        self.logger.debug(
            f"Required {elapsed}[s] to create the reprojected WorkUnit: {self.reprojected_wu_filepath}"
        )

        return self.reprojected_wu_filepath

    def _get_params_from_uri_file(self):
        """
        Get parameters we place into URI file as comments at the top.
        Example start of URI file (6/6/2024 COC):
        #desired_dates=['2019-04-02', '2019-05-07']
        #dist_au=42.0
        #patch_size=[20, 20]
        #patch_id=5845
        #patch_center_coords=(216.49999999999997, -13.500000000000005)
        #patch_box=[[216.33333333333331, -13.666666666666671], [216.33333333333331, -13.333333333333337], [216.66666666666666, -13.333333333333337], [216.66666666666666, -13.666666666666671], [216.33333333333331, -13.666666666666671]]
        /gscratch/dirac/DEEP/repo/DEEP/20190507/A0b/science#step6/20240425T145342Z/differenceExp/20190508/VR/VR_DECam_c0007_6300.0_2600.0/855719/differenceExp_DECam_VR_VR_DECam_c0007_6300_0_2600_0_855719_S12_DEEP_20190507_A0b_scienceHASHstep6_20240425T145342Z.fits
        6/6/2024 COC
        """
        results = {}
        with open(self.uri_filepath, "r") as f:
            for line in f:
                line = line.strip()
                if line == "":
                    continue  # deal with rogue blank lines or invisible double line endings
                if not line.startswith("#"):
                    break  # comments section is done
                line = line.lstrip("#").split("=")
                lhs = line[0].strip()
                rhs = line[1].strip()
                try:
                    rhs = eval(rhs)
                except ValueError:
                    self.logger.debug(f"Unable to eval() {lhs} field with value {rhs}.")
                    continue
                results[lhs] = rhs
        return results

    def _patch_arcmin_to_pixels(self, patch_size_arcmin, pixel_scale_arcsec_per_pix):
        """Take an array of two dimensions, in arcminutes, and convert this to
        pixels using the supplied pixel scale (in arcseconds per pixel).

        Parameters
        ----------
        patch_size_arcmin : nd.array
            A 2d array with shape (2,1) containing the width and height of the
            patch in arcminutes.
        pixel_scale_arcsec_per_pix : float
            The pixel scale in arcseconds per pixel.

        Returns
        -------
        nd.array
            A 2d array with shape (2,1) containing the width and height of the
            patch in pixels.
        """
        patch_pixels = int(np.ceil(self.patch_size * 60 / self.pixel_scale))

        self.logger.debug(
            f"Derived patch_pixels (w, h) = {patch_pixels} from patch_size_arcmin={patch_size_arcmin} and pixel_scale_arcsec_per_pix={pixel_scale_arcsec_per_pix}."
        )

        return patch_pixels

    def _create_wcs_from_corners(
        self,
        corners=None,
        image_width=None,
        image_height=None,
        pixel_scale=None,
        verbose=True,
    ):
        """
        Create a WCS object given the RA, Dec coordinates of the four corners of the image
        and the dimensions of the image in pixels. Optionally save as a FITS file.

        Parameters:
        corners (list of lists): [[RA1, Dec1], [RA2, Dec2], [RA3, Dec3], [RA4, Dec4]]
        image_width (int): Width of the image in pixels; if none, pixel_scale will be used to determine size.
        image_height (int): Height of the image in pixels; if none, pixel_scale will be used to determine size.
        filename (str): The name of the FITS file to save
        pixel_scale (float): The pixel scale (in units of arcseconds per pixel); used if image_width or image_height is None.
        verbose (bool): Print more messages.

        Returns:
        WCS: The World Coordinate System object for the image

        5/6/2024 COC + ChatGPT4
        """
        # TODO switch to https://docs.astropy.org/en/stable/api/astropy.wcs.utils.fit_wcs_from_points.html
        # Extract the corners
        if verbose:
            self.logger.debug(f"At the start, corners={corners}, type={type(corners)}")
        if type(corners) == type((0, 1)) or len(corners) == 1:
            corners = corners[0]
            self.logger.debug(f"After un-tuple corners are {corners}.")
        corners = np.unique(
            corners, axis=0
        )  # eliminate duplicate coords in case someone passes the repeat 1st corner used for plotting 6/5/2024 COC/DO
        if len(corners) != 4:
            raise ValueError(f"There should be four (4) corners. We saw: {corners}")
        ra = [corner[0] for corner in corners]
        dec = [corner[1] for corner in corners]
        #
        # Calculate the central position (average of the coordinates)
        center_ra = np.mean(ra)
        center_dec = np.mean(dec)
        #
        # Calculate the pixel scale in degrees per pixel
        if pixel_scale is not None:
            pixel_scale_ra = pixel_scale / 60 / 60
            pixel_scale_dec = pixel_scale / 60 / 60
        else:
            ra_range = max(ra) - min(
                ra
            )  # * np.cos(np.radians(center_dec))  # Adjust RA difference for declination; do not use cos(), results in incorrect pixel scale 6/6/2024 COC
            dec_range = max(dec) - min(dec)
            pixel_scale_ra = ra_range / image_width
            pixel_scale_dec = dec_range / image_height
        if verbose:
            self.logger.debug(
                f'Saw (RA,Dec) pixel scales ({pixel_scale_ra*60*60},{pixel_scale_dec*60*60})"/pixel. User-supplied: {pixel_scale}"/pixel.'
            )
        # Initialize a WCS object with 2 axes (RA and Dec)
        wcs = WCS(naxis=2)
        wcs.wcs.crpix = [image_width / 2, image_height / 2]
        wcs.wcs.crval = [center_ra, center_dec]
        wcs.wcs.cdelt = [
            -pixel_scale_ra,
            pixel_scale_dec,
        ]  # RA pixel scale might need to be negative (convention)
        # Rotation matrix, assuming no rotation
        wcs.wcs.pc = [[1, 0], [0, 1]]
        # Define coordinate frame and projection
        wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]
        wcs.array_shape = (image_height, image_width)

        return wcs
