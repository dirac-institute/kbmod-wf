import argparse
import os

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

from kbmod_wf.workflow_tasks import create_manifest, kbmod_search


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "sharded_reproject"]),
    ignore_for_cache=["logging_file"],
)
def reproject_wu(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger

    logger = get_configured_logger("task.reproject_wu", logging_file.filepath)

    from kbmod_wf.task_impls.reproject_multi_chip_multi_night_wu import reproject_wu

    guess_dist = inputs[1]  # heliocentric guess distance in AU
    logger.info(f"Starting reproject_ic for guess distance {guess_dist}")
    with ErrorLogger(logger):
        reproject_wu(
            guess_dist,
            ic_filepath=inputs[0],
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

        # reproject each WorkUnit for a range of distances
        reproject_futures = []
        repro_wu_filenames = []
        runtime_config = app_configs.get("reproject_wu", {})
        if "helio_guess_dists" not in runtime_config:
            raise ValueError("No 'helio_guess_dists' were provided in the runtime config for reprojection.")

        with open(create_manifest_future.result(), "r") as f:
            for line in f:
                collection_file = File(line.strip())
                wu_filename = line + ".wu"
                # Get the requested heliocentric guess distances (in AU) for reflex correction.
                distances = runtime_config["helio_guess_dists"]
                for dist in distances:
                    output_filename = wu_filename + f".{dist}.repro"
                    repro_wu_filenames.append(output_filename)
                    reproject_futures.append(
                        reproject_wu(
                            inputs=[collection_file, dist],
                            outputs=[File(output_filename)],
                            runtime_config=runtime_config,
                            logging_file=logging_file,
                        )
                    )

        # run kbmod search on each reprojected WorkUnit
        search_futures = []
        for i in range(len(reproject_futures)):
            f = reproject_futures[i]
            search_futures.append(
                kbmod_search(
                    inputs=[f],
                    outputs=[File(repro_wu_filenames[i] + ".search.parquet")],
                    runtime_config=app_configs.get("kbmod_search", {}),
                    logging_file=logging_file,
                )
            )

        for f in search_futures:
            # Apply a blocking call to ensure that the workflow does not exit before all futures are completed.
            # We use a try-catch so that any single future cannot crash the parent process.
            try:
                f.result()
            except Exception as e:
                logger.error(f"Error occurred while processing a future: {e}")

        logger.info("Workflow complete")

    parsl.clear()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        type=str,
        choices=["dev", "klone", "usdf"],
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
