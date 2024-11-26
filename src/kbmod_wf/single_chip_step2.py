import logging

from kbmod_wf.utilities import (
    LOGGING_CONFIG,
    apply_runtime_updates,
    get_resource_config,
    get_executors,
    get_configured_logger,
    ErrorLogger
)

logging.config.dictConfig(LOGGING_CONFIG)

import argparse
import os
import glob

import toml
import parsl
from parsl import python_app, File
import parsl.executors
import time


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "ckpt_32gb_2cpu_1gpu"]),
    ignore_for_cache=["logging_file"],
)
def step2(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    """Load an resampled WorkUnit and search through it.

    Parameters
    ----------
    inputs : `tuple` or `list`
        Order sensitive input to the Python App.
    outputs : `tuple` or `list`
        Order sensitive output of the Python App.
    runtime_config : `dict`, optional
        Runtime configuration values. No values are consumed.
    logging_file : `File` or `None`, optional
        Parsl File object poiting to the output logging file.

    Returns
    -------
    outputs : `tuple` or `list`
        Order sensitive output of the Python App.

    Inputs
    ----------
    wu_file : `File`
         Parsl File object pointing to the WorkUnit.
    ic_file : `File`
         Parsl File object poiting to the associated ImageCollection.

    Outputs
    -------
    results : `File`
        Parsl File object poiting to the results.
    """
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("workflow.step2", logging_file.filepath)

    import json

    from kbmod import ImageCollection
    from kbmod.work_unit import WorkUnit
    from kbmod.run_search import SearchRunner

    with ErrorLogger(logger):
        wu_path = inputs[0][0].filepath
        coll_path = inputs[1].filepath

        # Run the search
        ic = ImageCollection.read(coll_path)
        ic.data.sort("mjd_mid")
        wu = WorkUnit.from_fits(wu_path)
        res = SearchRunner().run_search_from_work_unit(wu)

        # add useful metadata to the results
        header = wu.wcs.to_header(relax=True)
        header["NAXIS1"], header["NAXIS2"] = wu.wcs.pixel_shape
        res.table.meta["wcs"] = json.dumps(dict(header))
        res.table.meta["visits"] = list(ic["visit"].data)
        res.table.meta["detector"] = ic["detector"][0]
        res.table.meta["mjd_mid"] = list(ic["mjd_mid"].data)
        res.table["uuid"] = [uuid.uuid4().hex for i in range(len(res.table))]

        # write results
        res.write_table(outputs[0].filepath, overwrite=True)

    return outputs


def workflow_runner(env=None, runtime_config={}):
    """Find all WorkUnits in the given directory and run KBMOD
    search on them.

    Requires matching image collections directory path.

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
    workflow_config = runtime_config.get("workflow", {})
    app_configs = runtime_config.get("apps", {})

    dfk = parsl.load(resource_config)
    logger = get_configured_logger("workflow.workflow_runner")
    
    if dfk:
        if runtime_config is not None:
            logger.info(f"Using runtime configuration definition:\n{toml.dumps(runtime_config)}")

        logger.info("Starting workflow")

        resampledwus_dirpath = "resampled_wus"
        imgcolls_dirpath = "collections"
        
        wufile_pattern = "*.wu"
        pattern = os.path.join(resampledwus_dirpath, wufile_pattern)
        wus = glob.glob(pattern)

        collnames, collfiles = [], []
        for wupth in wus:
            wuname = os.path.basename(wupth)
            wuname = wuname.split(".")[0]
            collnames.append(wuname)
            pattern = os.path.join(imgcolls_dirpath, wuname) + "*"
            collfiles.extend(glob.glob(pattern))
            
        logger.info(f"Found {len(wus)} WorkUnits in {resampledwus_path}")

        # Register step 2 for each output of step 1
        results = []
        for resample, collname, collfile in zip(wus, collnames, collfiles):
            logger.info(f"Registering {collname} for step2 of {collfile.filepath}")
            logging_file = File(f"logs/{collname}.search.log")
            results.append(
                step2(
                    inputs=[resample, collfile],
                    outputs=[File(f"results/{collname}.results.ecsv"),],
                    runtime_config=app_configs.get("step2", {}),
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
