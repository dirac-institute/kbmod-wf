from kbmod import ImageCollection

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


def ic_to_wu(ic_file=None, uri_file=None, runtime_config={}, wu_file=None, logger=None):
    ic_to_wu_converter = ICtoWUConverter(
        ic_file=ic_file, uri_file=uri_file, runtime_config=runtime_config, wu_file=wu_file, logger=logger
    )

    return ic_to_wu_converter.create_work_unit()


class ICtoWUConverter:
    def __init__(self, ic_file=None, uri_file=None, runtime_config={}, wu_file=None, logger=None):
        self.ic_file = ic_file
        self.uri_file = uri_file
        self.runtime_config = runtime_config
        self.wu_file = wu_file
        self.logger = logger

        self.overwrite = self.runtime_config.get("overwrite", False)
        self.search_config = self.runtime_config.get("search_config", None)

        # Default to 8 workers if not in the config. Value must be 0<num workers<65.
        # self.n_workers = np.max(1, np.min(self.runtime_config.get("n_workers", 8), 64))

        # ! Decide if we want these from a runtime_config or exclusively read from the uri file
        # self.patch_corners = self.runtime_config.get("patch_corners", None)
        # self.guess_dist = self.runtime_config.get("guess_dist", None)
        # self.pixel_scale = self.runtime_config.get("pixel_scale", None)

        # ! Decide if we want these from a runtime_config or calculate each time based on self.uri_params
        # self.image_width = self.runtime_config.get("image_width", None)
        # self.image_height = self.runtime_config.get("image_height", None)

        # self.uri_params = self._get_params_from_uri_file(uri_file=self.uri_file)

        # self.pixel_scale = self.uri_params['pixel_scale']

        # handle image dimensions
        # image_width = self.image_width
        # image_height = self.image_height
        # if image_width == None or image_height == None:
        #     if 'patch_box' not in self.uri_params:
        #         raise KeyError(f'Must supply image dimensions (image_width, image_height) or #patch_size= must be in a specified URI file.')
        #     if self.pixel_scale == None:
        #         raise KeyError(f'When patch pixel dimensions are not specifified, the user must supply a pixel scale via the command line or the uri file.')

        # image_width, image_height = self._patch_arcmin_to_pixels(
        #     patch_size_arcmin=self.uri_params["patch_size"],
        #     pixel_scale_arcsec_per_pix=self.pixel_scale,
        # )

        # print_debug(f'(image_width, image_height) is ({image_width}, {image_height}).')

        # copied from the third "if __name__== '__main__':" block in the original
        # print_debug(f"Creating WCS from patch")

        # Create a WCS object for the patch. This will be our common reprojection WCS
        # patch_fits_name = f"patch.fits"
        # if 'patch_id' in self.uri_params:
        #     patch_fits_name = f"patch_{self.uri_params['patch_id']}.fits"
        #
        # patch_wcs = self._create_wcs_from_corners(
        #     self.patch_corners,
        #     image_width,
        #     image_height,
        #     pixel_scale=self.pixel_scale,
        #     save_fits=True,
        #     filename=os.path.join(args.result_dir, patch_fits_name)
        # )

    # ======================
    # Seems like below here in the __init__ method, belong in the `WUReprojector class` in the `reproject_wu.py` file.
    # ======================

    # else:
    #     print(f'Reading existing WorkUnit from disk: {orig_wu_file}')
    #     orig_wu = WorkUnit.from_fits(orig_wu_file)
    #     elapsed = round(time.time() - last_time,1)
    #     print_debug(f"{elapsed} seconds to read original WorkUnit ({orig_wu_file}).")

    # # gather elements needed for reproject phase
    # imgs = orig_wu.im_stack
    # point_on_earth = EarthLocation(1814303.74553723, -5214365.7436216, -3187340.56598756, unit='m')

    # # Find the EBD (estimated barycentric distance) WCS for each image

    # last_time = time.time()
    # ebd_per_image_wcs, geocentric_dists = transform_wcses_to_ebd(
    #     orig_wu._per_image_wcs,
    #     imgs.get_single_image(0).get_width(),
    #     imgs.get_single_image(0).get_height(),
    #     self.guess_dist, # args.guess_dist,
    #     [astropy.time.Time(img.get_obstime(), format='mjd') for img in imgs.get_images()],
    #     point_on_earth,
    #     npoints=10,
    #     seed=None,
    # )
    # elapsed = round(time.time() - last_time, 1)
    # print_debug(f"{elapsed} seconds elapsed for transform WCS objects to EBD phase.")

    # if len(orig_wu._per_image_wcs) != len(ebd_per_image_wcs):
    #     raise ValueError(f"Number of barycentric WCS objects ({len(ebd_per_image_wcs)}) does not match the original number of images ({len(orig_wu._per_image_wcs)}).")

    # # Construct a WorkUnit with the EBD WCS and provenance data
    # print_debug(f"Creating Barycentric WorkUnit...")
    # last_time = time.time()
    # ebd_wu = WorkUnit(
    #     im_stack=orig_wu.im_stack,
    #     config=orig_wu.config,
    #     per_image_wcs=orig_wu._per_image_wcs,
    #     per_image_ebd_wcs=ebd_per_image_wcs,
    #     heliocentric_distance=guess_dist,#args.guess_dist,
    #     geocentric_distances=geocentric_dists,
    # )
    # elapsed = round(time.time() - last_time, 1)
    # print_debug(f"{elapsed} seconds elapsed to create EBD WorkUnit.")

    # del orig_wu # 6/7/2024 COC

    # # Reproject to a common WCS using the WCS for our patch
    # print_debug(f"Reprojecting WorkUnit with {n_workers} workers...")
    # last_time = time.time()
    # reprojected_wu = reprojection.reproject_work_unit(ebd_wu, patch_wcs, frame="ebd", max_parallel_processes=n_workers)
    # elapsed = round(time.time() - last_time, 1)
    # print_debug(f"{elapsed} seconds elapsed to create the reprojected WorkUnit.")
    # #   print_debug("Reprojected WorkUnit created")

    # # Save the reprojected WorkUnit
    # reprojected_wu_file = os.path.join(args.result_dir, "reprojected_wu.fits")
    # print_debug(f"Saving reprojected work unit to: {reprojected_wu_file}")
    # last_time = time.time()
    # reprojected_wu.to_fits(reprojected_wu_file)
    # elapsed = round(time.time() - last_time, 1)
    # print_debug(f"{elapsed} seconds elapsed to create the reprojected WorkUnit: {reprojected_wu_file}")

    # def _get_params_from_uri_file(self, uri_file):
    #     """
    #     Get parameters we place into URI file as comments at the top.
    #     Example start of URI file (6/6/2024 COC):
    #     #desired_dates=['2019-04-02', '2019-05-07']
    #     #dist_au=42.0
    #     #patch_size=[20, 20]
    #     #patch_id=5845
    #     #patch_center_coords=(216.49999999999997, -13.500000000000005)
    #     #patch_box=[[216.33333333333331, -13.666666666666671], [216.33333333333331, -13.333333333333337], [216.66666666666666, -13.333333333333337], [216.66666666666666, -13.666666666666671], [216.33333333333331, -13.666666666666671]]
    #     /gscratch/dirac/DEEP/repo/DEEP/20190507/A0b/science#step6/20240425T145342Z/differenceExp/20190508/VR/VR_DECam_c0007_6300.0_2600.0/855719/differenceExp_DECam_VR_VR_DECam_c0007_6300_0_2600_0_855719_S12_DEEP_20190507_A0b_scienceHASHstep6_20240425T145342Z.fits
    #     6/6/2024 COC
    #     """
    #     results = {}
    #     with open(uri_file, 'r') as f:
    #         for line in f:
    #             line = line.strip()
    #             if line == '': continue # deal with rogue blank lines or invisible double line endings
    #             if not line.startswith('#'): break # comments section is done
    #             line = line.lstrip('#').split('=')
    #             lhs = line[0].strip()
    #             rhs = line[1].strip()
    #             try:
    #                 rhs = eval(rhs)
    #             except ValueError:
    #                 self.logger.debug(f'Unable to eval() {lhs} field with value {rhs}.')
    #                 continue
    #             results[lhs] = rhs
    #     return results

    # def _patch_arcmin_to_pixels(self, patch_size_arcmin, pixel_scale_arcsec_per_pix):
    #     """
    #     Take an array of two dimensions, in arcminutes, and convert this to pixels using the supplied pixel scale (in arcseconds per pixel).
    #     6/6/2024 COC
    #     """
    #     x_pixels = int(np.ceil( (patch_size_arcmin[0]*60)/pixel_scale_arcsec_per_pix ))
    #     y_pixels = int(np.ceil( (patch_size_arcmin[1]*60)/pixel_scale_arcsec_per_pix ))

    #     patch_pixels = int( np.ceil( patch_size_arcmin * 60 / pixel_scale_arcsec_per_pix ) )
    #     patch_pixels = [ x_pixels, y_pixels ]

    #     self.logger.debug(
    #         f"Derived patch_pixels = {patch_pixels} from patch_size_arcmin={patch_size_arcmin} and pixel_scale_arcsec_per_pix={pixel_scale_arcsec_per_pix}."
    #     )

    #     return x_pixels, y_pixels

    # def _create_wcs_from_corners(self, corners=None, image_width=None, image_height=None, save_fits=False, filename='output.fits', pixel_scale=None, verbose=True):
    #     """
    #     Create a WCS object given the RA, Dec coordinates of the four corners of the image
    #     and the dimensions of the image in pixels. Optionally save as a FITS file.

    #     Parameters:
    #     corners (list of lists): [[RA1, Dec1], [RA2, Dec2], [RA3, Dec3], [RA4, Dec4]]
    #     image_width (int): Width of the image in pixels; if none, pixel_scale will be used to determine size.
    #     image_height (int): Height of the image in pixels; if none, pixel_scale will be used to determine size.
    #     save_fits (bool): If True, save the WCS to a FITS file
    #     filename (str): The name of the FITS file to save
    #     pixel_scale (float): The pixel scale (in units of arcseconds per pixel); used if image_width or image_height is None.
    #     verbose (bool): Print more messages.

    #     Returns:
    #     WCS: The World Coordinate System object for the image

    #     5/6/2024 COC + ChatGPT4
    #     """
    #     # TODO switch to https://docs.astropy.org/en/stable/api/astropy.wcs.utils.fit_wcs_from_points.html
    #     # Extract the corners
    #     if verbose: print_debug(f'At the start, corners={corners}, type={type(corners)}')
    #     if type(corners) == type((0,1)) or len(corners) == 1:
    #         corners = corners[0]
    #         print_debug(f'After un-tuple corners are {corners}.')
    #     corners = np.unique(corners, axis=0) # eliminate duplicate coords in case someone passes the repeat 1st corner used for plotting 6/5/2024 COC/DO
    #     if len(corners) != 4:
    #         raise ValueError(f'There should be four (4) corners. We saw: {corners}')
    #     ra = [corner[0] for corner in corners]
    #     dec = [corner[1] for corner in corners]
    #     #
    #     # Calculate the central position (average of the coordinates)
    #     center_ra = np.mean(ra)
    #     center_dec = np.mean(dec)
    #     #
    #     # Calculate the pixel scale in degrees per pixel
    #     if pixel_scale is not None:
    #         pixel_scale_ra = pixel_scale/60/60
    #         pixel_scale_dec = pixel_scale/60/60
    #     else:
    #         ra_range = (max(ra) - min(ra)) # * np.cos(np.radians(center_dec))  # Adjust RA difference for declination; do not use cos(), results in incorrect pixel scale 6/6/2024 COC
    #         dec_range = max(dec) - min(dec)
    #         pixel_scale_ra = ra_range / image_width
    #         pixel_scale_dec = dec_range / image_height
    #     if verbose: print_debug(f'Saw (RA,Dec) pixel scales ({pixel_scale_ra*60*60},{pixel_scale_dec*60*60})"/pixel. User-supplied: {pixel_scale}"/pixel.')
    #     # Initialize a WCS object with 2 axes (RA and Dec)
    #     wcs = WCS(naxis=2)
    #     wcs.wcs.crpix = [image_width / 2, image_height / 2]
    #     wcs.wcs.crval = [center_ra, center_dec]
    #     wcs.wcs.cdelt = [-pixel_scale_ra, pixel_scale_dec]  # RA pixel scale might need to be negative (convention)
    #     # Rotation matrix, assuming no rotation
    #     wcs.wcs.pc = [[1, 0], [0, 1]]
    #     # Define coordinate frame and projection
    #     wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    #     wcs.array_shape = (image_height, image_width)
    #     if save_fits:
    #         # Create a new FITS file with the WCS information and a dummy data array
    #         hdu = fits.PrimaryHDU(data=np.ones((image_height, image_width)), header=wcs.to_header())
    #         hdulist = fits.HDUList([hdu])
    #         hdulist.writeto(filename, overwrite=True)
    #         print_debug(f"Saved FITS file with WCS to {filename}")
    #     return wcs

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
            orig_wu = ic.toWorkUnit(
                config=kbmod.configuration.SearchConfiguration.from_file(self.search_config)
            )
            elapsed = round(time.time() - last_time, 1)
            self.logger.info(f"{elapsed} seconds to create WorkUnit.")

            last_time = time.time()
            orig_wu.to_fits(self.wu_file, overwrite=True)
            elapsed = round(time.time() - last_time, 1)
            self.logger.info(f"Saving original work unit to: {self.wu_file}")
            self.logger.info(f"{elapsed} seconds to write WorkUnit to disk: {self.wu_file}")

        return self.wu_file
