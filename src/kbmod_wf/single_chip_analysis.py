import logging

from kbmod_wf.utilities import (
    LOGGING_CONFIG,
    apply_runtime_updates,
    get_resource_config,
    get_executors,
    get_configured_logger,
    ErrorLogger,
    parse_logdir,
    plot_campaign
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


#"ckpt_2gb_2cpus", "ckpt_2gb_2cpus", "astro_2gb_2cpus"]),
@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "ckpt_4gb_2cpus"]),  
    ignore_for_cache=["logging_file"],
)
def postscript(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    """Run postscript actions after each individual task.

    Generally consists of creating analysis plots for each result.

    Parameters
    ----------
    inputs : `tuple` or `list`
        Order sensitive input to the Python App.
    outputs : `tuple` or `list`
        Order sensitive output of the Python App.
    runtime_config : `dict`, optional
        Runtime configuration values. No keys are consumed.
    logging_file : `File` or `None`, optional
        Parsl File object poiting to the output logging file.

    Returns
    -------
    outputs : `tuple` or `list`
        Order sensitive output of the Python App.

    Inputs
    ----------
    result_file : `File`
         Parsl File object poiting to the associated Results.

    Outputs
    -------
    results : `File`
        Parsl File object poiting to the results.
    """
    import tempfile
    import tarfile
    import json
    
    from astropy.table import Table
    from astropy.io import fits as fitsio
    from astropy.wcs import WCS
    import matplotlib.pyplot as plt
    
    from kbmod_wf.task_impls.deep_plots import (
        Figure,
        configure_plot,
        plot_result,
        select_known_objects
    )

    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("kbmod", logging_file.filepath)

    with ErrorLogger(logger):    
        # Grab some names from the input so we know how to name
        # our output plots etc.
        results_path = inputs[0].filepath
        collname = os.path.basename(results_path).split(".")[0]
        results = Table.read(results_path)
        
        # Grab external resources required
        # - wcs, times, visitids, fakes, known objects and so on
        obstimes = results.meta["mjd_mid"]
        wcs = WCS(json.loads(results.meta["wcs"]))

        fakes = fitsio.open(runtime_config.get(
            "fake_object_catalog", "fakes_catalog.fits"
        ))
        allknowns = Table.read(runtime_config.get(
            "known_object_catalog", "known_objects_catalog.fits"
        ))

        fakes, knowns = select_known_objects(fakes, allknowns, results)
        fakes = fakes.group_by("ORBITID")
        knowns = knowns.group_by("Name")

        # Make the plots, write them to tmpdir and tar them up
        allplots = []
        tmpdir = tempfile.mkdtemp()
        logger.info(f"Creating analysis plots for results of length: {len(results)}")
        for i, res in enumerate(results):
            figure = configure_plot(wcs, fig_kwargs={"figsize": (24, 12)})
            figure.fig.suptitle(f"{collname}, {res['uuid']}")
            figure = plot_result(figure, res, fakes, knowns, wcs, obstimes)

            pltname = f"{collname}_L{int(res['likelihood']):0>4}_idx{i:0>4}.jpg"
            pltpath = os.path.join(tmpdir, pltname)
            allplots.append(pltpath)
            logger.info(f"Saving {pltpath}")
            plt.savefig(pltpath)
            plt.close(figure.fig)
            
        with tarfile.open(outputs[0].filepath, "w|bz2") as tar:
            for f in allplots:
                tar.add(f)

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

        results_dirpath = "results"
        pattern = os.path.join(results_dirpath, "*results*")
        results = glob.glob(pattern)

        resampledwus_dirpath = "resampled_wus"
        imgcolls_dirpath = "collections"

        collnames, collfiles, wufiles = [], [], []
        for respth in results:
            resname = os.path.basename(respth).split(".")[0]
            collnames.append(resname)

            pattern = os.path.join(imgcolls_dirpath, resname) + "*"
            collfiles.extend(glob.glob(pattern))
            
            pattern = os.path.join(resampledwus_dirpath, resname) + "*"
            wufiles.extend(glob.glob(pattern))

        logger.info("Starting workflow")
        logger.info(f"Found {len(results)} files in {results_dirpath}")

        # Register postscript for each output of step 2
        analysis = []
        for result, collname in zip(results, collnames):
            logger.info(f"Registering {collname} for postscript")
            logging_file = File(f"analysis_logs/{collname}.analysis.log")
            plots_archive = File(f"analysis_plots/{collname}.plots.tar.bz2")
            analysis.append(
                postscript(
                    inputs=[File(result)],
                    outputs=[plots_archive, ],
                    runtime_config=app_configs.get("postscript", {}),
                    logging_file=logging_file
                )
            )
    
        [f.result() for f in analysis]
        dfk.wait_for_current_tasks()
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
