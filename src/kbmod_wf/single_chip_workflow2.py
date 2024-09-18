import argparse
import os
import glob

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

from kbmod_wf.workflow_tasks import create_manifest, ic_to_wu, kbmod_search


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "sharded_reproject"]),
    ignore_for_cache=["logging_file"],
)
def step1(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("kbmod.ic_to_wu")

    import numpy as np
    from reproject.mosaicking import find_optimal_celestial_wcs

    from kbmod import ImageCollection
    from kbmod.configuration import SearchConfiguration
    import kbmod.reprojection as reprojection

    from lsst.daf.butler import Butler
    
    logger.info(f"Running with config {runtime_config}")

    with ErrorLogger(logger):
        # Unravell inputs
        repo_root = runtime_config["butler_config_filepath"]
        search_conf_path = runtime_config.get("search_config_filepath", None)
        ic_file = inputs[0].filepath

        # Run core tasks
        logger.info("Reading ImageCollection and adjusting search limits.")
        ic = ImageCollection.read(ic_file)
        ic.data.sort("mjd_mid")
        butler = Butler(repo_root)
        search_conf = SearchConfiguration.from_file(search_conf_path)

        # fixup config to match specifics of the image collection in question
        search_conf._params["n_obs"] = len(ic)/2
        logger.info(f"Setting search config n_obs to {search_conf._params['n_obs']}")
        
        # Fit the optimal WCS
        # TODO: triple check this doesn't flip the array, I'm pretty sure it does
        opt_wcs, shape = find_optimal_celestial_wcs(list(ic.wcs))
        opt_wcs.array_shape = shape

        wu = ic.toWorkUnit(search_config=search_conf, butler=butler, overwrite=True)
        logger.info("Created a WorkUnit")

        # we've got everything we wanted out of IC, clean it up.
        del ic

        # Resample the work unit so all pixels point to the same (ra, dec)
        logger.info(f"Writing resampled wu to {outputs[0]}")
        resampled_wu = reprojection.reproject_work_unit(
            wu,
            opt_wcs,
            max_parallel_processes=runtime_config.get("n_workers", 8),
        )
        resampled_wu.to_fits(outputs[0])

    return outputs


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "gpu"]),
    ignore_for_cache=["logging_file"],
)
def step2(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("kbmod.search_task", logging_file.filepath)

    import json
    
    from kbmod.work_unit import WorkUnit
    from kbmod.run_search import SearchRunner

    with ErrorLogger(logger):
        wu = WorkUnit.from_fits(inputs[0].filename)
        res = SearchRunner().run_search_from_work_unit(wu)
        header = wu.wcs.to_header(relax=True)
        h, w = wu.wcs.pixel_shape
        header["NAXIS1"], header["NAXIS2"] = h, w
        res.table.meta["wcs"] = json.dumps(dict(header))
        res.write_table(outputs[0].filename)

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
                              
    if dfk:
        logging_file = File(os.path.join(dfk.run_dir, "kbmod.log"))
        logger = get_configured_logger("workflow.workflow_runner", logging_file.filepath)

        if runtime_config is not None:
            logger.info(f"Using runtime configuration definition:\n{toml.dumps(runtime_config)}")

        logger.info("Starting workflow")
        
        directory_path = runtime_config.get("staging_directory", ".")
        file_pattern = runtime_config.get("file_pattern", "*.collection")
        pattern = os.path.join(directory_path, file_pattern)
        entries = glob.glob(pattern)
        logger.info(f"Found {len(entries)} files in {directory_path}")

        step1_futures = []
        for collection in entries:
            collection_file = File(collection)
            collname = os.path.basename(collection)
            collname = collname.split(".")[0]
            step1_futures.append(
                step1(
                    inputs=[collection_file],
                    outputs=[File(f"{collname}_resampled.wu")],
                    runtime_config=app_configs.get("ic_to_wu", {}),
                    logging_file=logging_file,
                )
            )

        # run kbmod search on each reprojected WorkUnit
        search_futures = []
        for resampled_future in step1_futures:
            search_futures.append(
                step2(
                    inputs=resampled_future.result(),
                    outputs=[File(resampled_future.result()[0].filepath + ".results.ecsv")],
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
