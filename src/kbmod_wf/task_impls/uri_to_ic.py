import os
import glob

# from kbmod import ImageCollection


def uri_to_ic(target_uris_file=None, uris_base_dir=None, ic_output_file=None, logger=None):
    with open(target_uris_file, "r") as f:
        for line in f:
            value = line.strip()
            logger.info(line.strip())

    with open(ic_output_file, "w") as f:
        f.write(f"Logged: {value}")

    return ic_output_file


# def uri_to_ic(target_uris_file=None, uris_base_dir=None, ic_output_file=None, logger=None):
#     # Load the list of images from our saved file "sample_uris.txt"
#     uris = []
#     with open(target_uris_file) as f:
#         for l in f.readlines():
#             l = l.strip()  # seeing invisible trailing characters 6/12/2024 COC
#             if l == "":
#                 continue  # skip blank lines 6/12/2024 COC
#             if not l.startswith("#"):
#                 # Ignore commented metadata
#                 uris.append(l)

#     if uris_base_dir is not None:
#         logger.debug(f"Using URIs base dir: {uris_base_dir}")
#         if not os.path.isdir(uris_base_dir):
#             logger.error(f"Invalid URIS base directory provided: {uris_base_dir}")
#             raise ValueError(f"Invalid URIS base directory provided: {uris_base_dir}")

#     # Clean up the URI strings
#     for i in range(len(uris)):
#         file_prefix = "file://"
#         curr = uris[i].replace("%23", "#").strip()
#         if curr.startswith(file_prefix):
#             curr = curr[len(file_prefix) :]
#         if uris_base_dir is not None:
#             curr = os.path.join(uris_base_dir, curr.lstrip(os.path.sep))
#         uris[i] = curr

#     # Make sure the files can all be found 6/12/2024 COC
#     for uri in uris:
#         if len(glob.glob(uri)) == 0:
#             raise FileNotFoundError(f"Could not find {uri}.")

#     logger.info("Creating ImageCollection")
#     # Create an ImageCollection object from the list of URIs
#     ic = ImageCollection.fromTargets(uris)
#     logger.info("ImageCollection created")

#     ic.write(ic_output_file, format="ascii.ecsv")
