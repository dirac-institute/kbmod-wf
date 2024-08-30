from parsl import python_app
from kbmod_wf.utilities.executor_utilities import get_executors


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "local_thread"]),
    ignore_for_cache=["logging_file"],
)
def create_manifest(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    """This app will go to a given directory, find all of the *.collection files there,
    and copy the paths to a manifest file.

    Parameters
    ----------
    inputs : tuple, optional
        No inputs required, by default ()
    outputs : tuple, optional
        Currently expects an iterable with 1 element - a parsl.File object that
        specifies where the manifest file will be written, by default ()
    runtime_config : dict, optional
        A dictionary of configuration setting specific to this task, by default {}
    logging_file : parsl.File, optional
        The parsl.File object the defines where the logs are written, by default None

    Returns
    -------
    parsl.File
        The file object that points to the manifest file that was created.

    Raises
    ------
    ValueError
        If the staging_directory is not provided in the runtime_config.
    """
    import glob
    import os
    import shutil

    from kbmod_wf.utilities.logger_utilities import get_configured_logger

    logger = get_configured_logger("task.create_manifest", logging_file.filepath)

    directory_path = runtime_config.get("staging_directory")
    output_path = runtime_config.get("output_directory")

    if directory_path is None:
        logger.error(f"No staging_directory provided in the configuration.")
        raise ValueError("No staging_directory provided in the configuration.")

    if output_path is None:
        logger.info(
            f"No output_directory provided in the configuration. Using staging directory: {directory_path}"
        )
        output_path = directory_path

    if not os.path.exists(output_path):
        logger.info(f"Creating output directory: {output_path}")
        os.makedirs(output_path)

    logger.info(f"Looking for staged files in {directory_path}")

    # Gather all the *.collection entries in the directory
    file_pattern = runtime_config.get("file_pattern", "*.collection")
    pattern = os.path.join(directory_path, file_pattern)
    entries = glob.glob(pattern)

    # Filter out directories, keep only files
    # Copy files to the output directory, and adds them to the list of files
    files = []
    for f in entries:
        if os.path.isfile(os.path.join(directory_path, f)):
            files.append(shutil.copy2(f, output_path))

    logger.info(f"Found {len(files)} files in {directory_path}")

    # Write the filenames to the manifest file
    logger.info(f"Writing manifest file: {outputs[0].filepath}")
    with open(outputs[0].filepath, "w") as manifest_file:
        for file in files:
            manifest_file.write(file + "\n")

    return outputs[0]
