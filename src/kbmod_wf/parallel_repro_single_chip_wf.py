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

from kbmod_wf.workflow_tasks import create_manifest, ic_to_wu_return_shards, kbmod_search


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "reproject_single_shard"]),
    ignore_for_cache=["logging_file"],
)
def reproject_shard(inputs=(), outputs=(), wcses=None, runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger

    logger = get_configured_logger("task.sharded_reproject", logging_file.filepath)

    from kbmod_wf.task_impls.reproject_single_chip_single_night_wu_shard import reproject_shard

    logger.info("Starting reproject_ic")
    with ErrorLogger(logger):
        reproject_shard(
            original_wu_shard_filepath=inputs[0].filepath,
            original_wcs=wcses,
            reprojected_wu_filepath=outputs[0].filepath,
            runtime_config=runtime_config,
            logger=logger,
        )
    logger.info("Completed reproject_ic")
    return outputs[0]


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
            runtime_config=app_configs.get("create_manifest", {}),
            logging_file=logging_file,
        )

        with open(create_manifest_future.result(), "r") as manifest:
            # process each .collection file in the manifest
            original_work_unit_futures = []
            for line in manifest:
                # Create path object for the line in the manifest
                input_file = Path(line.strip())

                # Create a directory to contain each work unit's shards
                sharded_directory = Path(input_file.parent, input_file.stem)
                sharded_directory.mkdir(exist_ok=True)

                # Construct the work unit filepath
                output_workunit_filepath = Path(sharded_directory, input_file.stem + ".wu")

                # Create the work unit future
                original_work_unit_futures.append(
                    ic_to_wu_return_shards(
                        inputs=[File(str(input_file))],
                        outputs=[File(str(output_workunit_filepath))],
                        runtime_config=app_configs.get("ic_to_wu", {}),
                        logging_file=logging_file,
                    )
                )

        # reproject each WorkUnit shard individually
        # For chip-by-chip, this isn't really necessary, so hardcoding to 0.
        reproject_futures = []
        for f in original_work_unit_futures:
            shard_futures = []
            shard_files, wcses = f.result()
            for i in shard_files:
                shard_file = Path(i)
                shard_future = reproject_shard(
                    inputs=[File(str(shard_file))],
                    outputs=[File(str(shard_file.parent / (shard_file.stem + ".repro")))],
                    wcses=wcses,
                    runtime_config=app_configs.get("reproject_wu", {}),
                    logging_file=logging_file,
                )
                shard_futures.append(shard_future)
            reproject_futures.append(shard_futures)

        # run kbmod search on each reprojected WorkUnit
        search_futures = []
        for f in reproject_futures:
            search_futures.append(
                kbmod_search(
                    inputs=[i.result() for i in f],
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
    if args.runtime_config is not None and os.path.exists(args.runtime_config):
        with open(args.runtime_config, "r") as toml_runtime_config:
            runtime_config = toml.load(toml_runtime_config)

    workflow_runner(env=args.env, runtime_config=runtime_config)
