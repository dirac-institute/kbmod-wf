import argparse
import os
from pathlib import Path

import toml
import parsl
from parsl import python_app, File
import parsl.executors

from kbmod_wf.utilities import (
    apply_runtime_updates,
    get_resource_config,
    get_executors,
    get_configured_logger,
)


@python_app(executors=get_executors(["local_dev_testing", "local_thread"]))
def create_manifest(inputs=(), outputs=(), runtime_config={}, logging_file=None):
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


@python_app(executors=get_executors(["local_dev_testing"]))
def ic_to_wu(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger

    logger = get_configured_logger("task.ic_to_wu", logging_file)

    logger.info("Starting ic_to_wu")
    with ErrorLogger(logger):
        # open the file
        input_file = Path(inputs[0])
        output_files = []
        with open(input_file, "r") as input_f:
            # read the number in the file
            data = input_f.read()

            # create a directory for the output with the same name as the input file
            output_directory = input_file.parent / input_file.stem
            output_directory.mkdir(exist_ok=True)

            # create indexed output files
            for i in range(0, int(data)):
                output_file = output_directory / f"{i}_{input_file.stem}.shard"
                with open(output_file, "w") as output_f:
                    output_f.write(f"Work Unit shard: {i}")
                    output_files.append(output_file)

    logger.info("Completed ic_to_wu")

    return output_files


@python_app(executors=get_executors(["local_dev_testing"]))
def reproject_wu(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    from time import sleep
    import random

    logger = get_configured_logger("task.ic_to_wu", logging_file)

    logger.info("Starting reproject_wu")
    sleep(random.uniform(1, 5))
    with open(outputs[0].filepath, "w") as f:
        f.write(f"Reprojected: {inputs[0]}")

    logger.info(f"Reprojected: {inputs[0]}")

    return (inputs[0].parent, outputs[0])


@python_app(executors=get_executors(["local_dev_testing"]))
def kbmod_search(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger

    logger = get_configured_logger("task.kbmod_search", logging_file)

    logger.info("Starting kbmod_search")
    logger.info("Pretending to run kbmod search")
    output_directory = Path(inputs[0][0])
    logger.info(f"Output directory: {output_directory}")
    search_file = output_directory / (output_directory.stem + "_search.ecsv")
    with open(search_file, "w") as f:
        f.write("Pretending to run kbmod search")
    logger.info("Completed kbmod_search")

    return search_file


def workflow_runner(env=None, runtime_config={}):
    """This function will load and configure Parsl, and run the workflow.

    Parameters
    ----------
    env : str, optional
        Environment string used to define which resource configuration to use,
        by default None
    runtime_config : dict, optional
        Dictionary of assorted runtime configuration parameters, by default {}
    """
    resource_config = get_resource_config(env=env)
    resource_config = apply_runtime_updates(resource_config, runtime_config)

    app_configs = runtime_config.get("apps", {})

    dfk = parsl.load(resource_config)
    if dfk:
        logging_file = File(os.path.join(dfk.run_dir, "kbmod.log"))
        logger = get_configured_logger("workflow.workflow_runner", logging_file.filepath)

        if runtime_config is not None:
            logger.info(f"Using runtime configuration definition:\n{toml.dumps(runtime_config)}")

        logger.info("Starting workflow")

        # gather all the *.collection files that are staged for processing
        create_manifest_config = app_configs.get("create_manifest", {})
        manifest_file = File(
            os.path.join(create_manifest_config.get("output_directory", os.getcwd()), "manifest.txt")
        )
        create_manifest_future = create_manifest(
            inputs=[],
            outputs=[manifest_file],
            runtime_config=create_manifest_config,
            logging_file=logging_file,
        )

        with open(create_manifest_future.result(), "r") as f:
            # process each .collection file in the manifest into a .wu file
            original_work_unit_futures = []
            for line in f:
                # Create path object for the line in the manifest
                input_file = Path(line.strip())

                # Create a directory for the sharded work unit files
                sharded_directory = Path(input_file.parent, input_file.stem)
                sharded_directory.mkdir(exist_ok=True)

                # Create the work unit filepath
                output_workunit_filepath = Path(sharded_directory, input_file.stem + ".wu")

                # Create the work unit future
                original_work_unit_futures.append(
                    ic_to_wu(
                        inputs=[input_file],
                        outputs=[File(output_workunit_filepath)],
                        runtime_config=app_configs.get("ic_to_wu", {}),
                        logging_file=logging_file,
                    )
                )

        reprojected_wu_futures = []
        for f in original_work_unit_futures:
            shard_futures = []
            for i in f.result():
                shard_future = reproject_wu(
                    inputs=[i],
                    outputs=[File(i.parent / (i.stem + ".repro"))],
                    runtime_config=app_configs.get("reproject_wu", {}),
                    logging_file=logging_file,
                )
                shard_futures.append(shard_future)
            reprojected_wu_futures.append(shard_futures)

        # run kbmod search on each reprojected WorkUnit
        search_futures = []
        for f in reprojected_wu_futures:
            search_futures.append(
                kbmod_search(
                    inputs=[i.result() for i in f],  #! This, surprisingly, seems to work!!!
                    outputs=[],
                    runtime_config=app_configs.get("kbmod_search", {}),
                    logging_file=logging_file,
                )
            )

        [f.result() for f in search_futures]

        logger.info("Workflow complete")

    parsl.clear()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        type=str,
        choices=["dev", "klone"],
        help="The environment to run the workflow in.",
    )

    parser.add_argument(
        "--runtime-config",
        type=str,
        help="The complete runtime configuration filepath to use for the workflow.",
    )

    args = parser.parse_args()

    # if a runtime_config file was provided and exists, load the toml as a dict.
    runtime_config = {}

    #! Don't forget to remove this hardcoded path!!!
    args.runtime_config = "/Users/drew/code/kbmod-wf/example_runtime_config.toml"
    if args.runtime_config is not None and os.path.exists(args.runtime_config):
        with open(args.runtime_config, "r") as toml_runtime_config:
            runtime_config = toml.load(toml_runtime_config)

    workflow_runner(env=args.env, runtime_config=runtime_config)
