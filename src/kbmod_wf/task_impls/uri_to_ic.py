import os
import glob
import time
from logging import Logger
from kbmod import ImageCollection


def placeholder_uri_to_ic(
    target_uris_file_path=None, uris_base_dir=None, ic_output_file_path=None, logger=None
):
    with open(target_uris_file_path, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    with open(ic_output_file_path, "w") as f:
        f.write(f"Logged: {value} - {time.time()}\n")

    return ic_output_file_path


#! I believe that we can remove the `uris_base_dir` parameter from the function
#! signature. It doesn't seem to be used in practice.
def uri_to_ic(
    uris_filepath: str = None,
    uris_base_dir: str = None,
    ic_filepath: str = None,
    runtime_config: dict = {},
    logger: Logger = None,
):
    """For each URI in the target_uris_file, perform string cleaning and build a
    file path. Then create an ImageCollection object with the final file paths
    and write the ImageCollection to a file.

    Parameters
    ----------
    uris_filepath : str, optional
        The fully resolved path to the input file containing the list of URIs, by default None
    uris_base_dir : str, optional
        The directory containing the files references by URI in the target_uris_file,
        by default None
    ic_filepath : str, optional
        The fully resolved path to the output file where the ImageCollection will
        be saved, by default None
    runtime_config : dict, optional
        Any parameters that must be set for this work to be performed, by default {}
    logger : Logger, optional
        The logger to use for this work, by default None

    Raises
    ------
    ValueError
        If a non-existent URIs base directory is provided
    FileNotFoundError
        If we're unable to find a file referenced by a URI in the target_uris_file
        after the URI has been cleaned up, and full path has been built
    """

    # Load the list of images from our saved file "sample_uris.txt"
    uris = []
    with open(uris_filepath) as f:
        for l in f.readlines():
            l = l.strip()  # seeing invisible trailing characters 6/12/2024 COC
            if l == "":
                continue  # skip blank lines 6/12/2024 COC
            if not l.startswith("#"):
                # Ignore commented metadata
                uris.append(l)

    if uris_base_dir is not None:
        logger.debug(f"Using URIs base dir: {uris_base_dir}")
        if not os.path.isdir(uris_base_dir):
            logger.error(f"Invalid URIS base directory provided: {uris_base_dir}")
            raise ValueError(f"Invalid URIS base directory provided: {uris_base_dir}")

    # Clean up the URI strings
    for i in range(len(uris)):
        # clean up character encoding
        curr = uris[i].replace("%23", "#").strip()

        # strip off the file:// prefix if it exists
        file_prefix = "file://"
        if curr.startswith(file_prefix):
            curr = curr[len(file_prefix) :]

        # if a base directory is provided, prepend it to the URI
        if uris_base_dir is not None:
            curr = os.path.join(uris_base_dir, curr.lstrip(os.path.sep))

        # check if the file exists
        if len(glob.glob(curr)) == 0:
            logger.error(f"Could not find file: {curr}.")
            raise FileNotFoundError(f"Could not find file: {curr}.")

        uris[i] = curr

    logger.info("Creating ImageCollection")
    # Create an ImageCollection object from the list of URIs
    ic = ImageCollection.fromTargets(uris)

    logger.info(f"Writing ImageCollection to file {ic_filepath}")
    ic.write(ic_filepath, format="ascii.ecsv")
