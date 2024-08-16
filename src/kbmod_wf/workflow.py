import argparse
import os
import toml
import parsl
from parsl import File
import parsl.executors

from kbmod_wf.utilities.configuration_utilities import apply_runtime_updates, get_resource_config
from kbmod_wf.utilities.logger_utilities import configure_logger

from kbmod_wf.workflow_tasks import create_manifest, ic_to_wu, kbmod_search, reproject_wu, uri_to_ic


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
        logging_file = File(os.path.join(dfk.run_dir, "parsl.log"))
        logger = configure_logger("workflow.workflow_runner", logging_file.filepath)

        if runtime_config is not None:
            logger.info(f"Using runtime configuration definition:\n{toml.dumps(runtime_config)}")

        logger.info("Starting workflow")

        # gather all the .lst files that are staged for processing
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

        with open(create_manifest_future.result(), "r") as f:
            # process each .lst file in the manifest into a .ecvs file
            uri_to_ic_futures = []
            uri_files = []
            for line in f:
                uri_file = File(line.strip())
                uri_files.append(uri_file)
                uri_to_ic_futures.append(
                    uri_to_ic(
                        inputs=[uri_file],
                        outputs=[File(line + ".ecsv")],
                        runtime_config=app_configs.get("uri_to_ic", {}),
                        logging_file=logging_file,
                    )
                )

        # create an original WorkUnit for each .ecsv file
        original_work_unit_futures = []
        for f in uri_to_ic_futures:
            original_work_unit_futures.append(
                ic_to_wu(
                    inputs=[f.result()],
                    outputs=[File(f.result().filepath + ".wu")],
                    runtime_config=app_configs.get("ic_to_wu", {}),
                    logging_file=logging_file,
                )
            )

        # reproject each WorkUnit for a range of distances
        reproject_futures = []
        for f, uri_file in zip(original_work_unit_futures, uri_files):
            for distance in range(40, 60, 10):
                reproject_futures.append(
                    reproject_wu(
                        inputs=[f.result(), uri_file],
                        outputs=[File(f.result().filepath + f".{distance}.repro")],
                        runtime_config=app_configs.get("reproject_wu", {}),
                        logging_file=logging_file,
                    )
                )

        # run kbmod search on each reprojected WorkUnit
        search_futures = []
        for f in reproject_futures:
            search_futures.append(
                kbmod_search(
                    inputs=[f.result()],
                    outputs=[File(f.result().filepath + ".search.ecsv")],
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
