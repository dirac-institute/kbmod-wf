import logging
logging.basicConfig(level=logging.INFO)

import argparse
import os
import glob

import toml
import parsl
from parsl import python_app, File
import parsl.executors
import time

from kbmod_wf.utilities import (
    apply_runtime_updates,
    get_resource_config,
    get_executors,
    get_configured_logger,
)


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "gpu"]),
    ignore_for_cache=["logging_file"],
)
def step2(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("task.step2", logging_file.filepath)

    import json
    
    from kbmod.work_unit import WorkUnit
    from kbmod.run_search import SearchRunner

    with ErrorLogger(logger):
        wu = WorkUnit.from_fits(inputs[0])
        res = SearchRunner().run_search_from_work_unit(wu)

        # a WCS in the results table would be very helpful
        # so add it in.
        header = wu.wcs.to_header(relax=True)
        h, w = wu.wcs.pixel_shape
        header["NAXIS1"], header["NAXIS2"] = h, w
        res.table.meta["wcs"] = json.dumps(dict(header))

        # write the results to a file
        res.write_table(outputs[0].filepath)

    return outputs


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
    logger = get_configured_logger("workflow.workflow_runner")
    
    if dfk:
        if runtime_config is not None:
            logger.info(f"Using runtime configuration definition:\n{toml.dumps(runtime_config)}")

        logger.info("Starting workflow")
        
        #directory_path = runtime_config.get("staging_directory", "resampled_wus")
        directory_path = "resampled_wus"
        file_pattern = "*.wu"
        pattern = os.path.join(directory_path, file_pattern)
        entries = glob.glob(pattern)
        logger.info(f"Found {len(entries)} files in {directory_path}")

        # run kbmod search on each reprojected WorkUnit
        search_futures = []
        for workunit in entries:
            wuname = os.path.basename(workunit)
            wuname = wuname.split(".")[0]
            open(f"logs/{wuname}.search.log", "w").close()
            logging_file = File(f"logs/{wuname}.search.log")
            search_futures.append(
                step2(
                    inputs=[workunit,],
                    outputs=[File(f"results/{wuname}.results.ecsv")],
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
