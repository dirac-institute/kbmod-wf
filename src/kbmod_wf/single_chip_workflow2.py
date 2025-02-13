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


# "esci_48_8cpus" "astro_48_8cpus"
@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "ckpt_48gb_8cpus"]),
    ignore_for_cache=["logging_file"],
)
def step1(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    """Create WorkUnit out of an ImageCollection and resample it.

    Parameters
    ----------
    inputs : `tuple` or `list`
        Order sensitive input to the Python App.
    outputs : `tuple` or `list`
        Order sensitive output of the Python App.
    runtime_config : `dict`, optional
        Runtime configuration values. Keys ``butler_config_filepath``,
        ``search_config_filepath`` and ``n_workers`` will be consumed
        if they exist.
    logging_file : `File` or `None`, optional
        Parsl File object poiting to the output logging file.

    Returns
    -------
    outputs : `tuple` or `list`
        Order sensitive output of the Python App.

    Inputs
    ----------
    ic_file : `File`
         Parsl File object pointing to the ImageCollection.

    Outputs
    -------
    workunit_path : `File`
        Parsl File object poiting to the resampled WorkUnit.
    """
    import numpy as np
    from reproject.mosaicking import find_optimal_celestial_wcs

    from kbmod import ImageCollection
    from kbmod.configuration import SearchConfiguration
    import kbmod.reprojection as reprojection

    from lsst.daf.butler import Butler

    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("workflow.step1", logging_file.filepath)

    with ErrorLogger(logger):
        # Unravell inputs
        repo_root = runtime_config["butler_config_filepath"]
        search_conf_path = runtime_config.get("search_config_filepath", None)
        ic_file = inputs[0].filepath

        ####
        #    Run core tasks
        ###
        ic = ImageCollection.read(ic_file)

        ###    Mask out images that we don't want or can not search through
        # mask out poor weather images
        #mask_zp = np.logical_and(ic["zeroPoint"] > 27 , ic["zeroPoint"] < 32)
        #ic = ic[np.logical_and(mask_zp, mask_wcs_err)]

        # mask out images with WCS error more than 0.1 arcseconds because we
        # can't trust their resampling can be correct
        mask_good_wcs_err = ic["wcs_err"] < 1e-04
        if not all(mask_good_wcs_err):
            logger.warning("Image collection contains large WCS errors!")
        #ic = ic[mask_good_wcs_err]
        #ic.reset_lazy_loading_indices()
        ic.data.sort("mjd_mid")

        ### Adjust the search parameters based on remaining metadata
        search_conf = SearchConfiguration.from_file(search_conf_path)
        if len(ic)//2 < 25:
            n_obs = 15
        else:
            n_obs = len(ic)//2
        search_conf._params["n_obs"] = n_obs

        ###    Resampling
        # Fit the optimal WCS
        opt_wcs, shape = find_optimal_celestial_wcs(list(ic.wcs))
        opt_wcs.array_shape = shape

        butler = Butler(repo_root)
        wu = ic.toWorkUnit(search_config=search_conf, butler=butler)
        del ic  # we're done with IC, clean it up for memory

        resampled_wu = reprojection.reproject_work_unit(
            wu,
            opt_wcs,
            parallelize=True,
            max_parallel_processes=runtime_config.get("n_workers", 8),
        )
        resampled_wu.to_fits(outputs[0].filepath, overwrite=True)

    return outputs


# "esci_48_2cpu_1gpu", "esci_48_2cpu_1gpu"
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
    import json

    from kbmod import ImageCollection
    from kbmod.work_unit import WorkUnit
    from kbmod.run_search import SearchRunner

    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("workflow.step2", logging_file.filepath)

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
    logger = get_configured_logger("workflow.postscript", logging_file.filepath)

    with ErrorLogger(logger):
        # Grab some names from the input so we know how to name
        # our output plots etc.
        results_path = inputs[0][0].filepath
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
    """Find all image collections in the given directory and run KBMOD
    search on them.

    Running the Workflow is a multi-step process which includes
    additional preparation and cleanup work that executes at the
    submit location:
    - Run prep
        - Load runtime config
        - find all files in ``staging_directory`` that match ``pattern``
        - filter out unwanted files
    - Run KBMOD Search for each remaining collection
    - Create a workflow Gantt chart.

    Running a KBMOD search is a 3 step process:
    - step 1, executed on CPUs
        - load ImageCollection
        - filter unwanted rows of data from it
        - load SearchConfiguration
        - update search config values based on the IC metadata
        - materialize a WorkUnit, requires the Rubin Data Butler
        - resample a WorkUnit, targets the largest common footprint WCS
        - writes the WorkUnit to file
    - step 2, executed on GPUs
        - loads the WorkUnit
        - runs KBMOD search
        - adds relevant metadata to the Results Table
        - writes Results to file
    - step 3, executed on CPUs
        - loads Results file
        - makes an analysis plot

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
        
        directory_path = workflow_config.get("staging_directory", "collections")
        file_pattern = workflow_config.get("ic_filename_pattern", "*.collection")
        pattern = os.path.join(directory_path, file_pattern)
        entries = glob.glob(pattern)
        logger.info(f"Found {len(entries)} files in {directory_path}")

        skip_ccds = workflow_config.get("skip_ccds", ["002", "031", "061"])

        # bookeping, used to build future output filenames
        collfiles, collnames, resampled_wus = [], [], []
        for collection in entries:
            if any([ccd in collection for ccd in skip_ccds]):
                logger.warning(f"Skipping {collection} bad detector.")
                continue

            # bookeeping for future tasks
            collname = os.path.basename(collection).split(".")[0]
            collnames.append(collname)

            # Register step 1 for each of the collection file
            logger.info(f"Registering {collname} for step1 of {collection}")
            logging_file = File(f"logs/{collname}.resample.log")
            collection_file = File(collection)
            collfiles.append(collection_file)
            resampled_wus.append(
                step1(
                    inputs=[collection_file],
                    outputs=[File(f"resampled_wus/{collname}.resampled.wu")],
                    runtime_config=app_configs.get("step1", {}),
                    logging_file=logging_file,
                )
            )

        # Register step 2 for each output of step 1
        results = []
        for resample, collname, collfile in zip(resampled_wus, collnames, collfiles):
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

        # Register postscript for each output of step 2
        analysis = []
        for result, collname in zip(results, collnames):
            logger.info(f"Registering {collname} for postscript")
            logging_file = File(f"logs/{collname}.analysis.log")
            plots_archive = File(f"plots/{collname}.plots.tar.bz2")
            analysis.append(
                postscript(
                    inputs=[result],
                    outputs=[plots_archive, ],
                    runtime_config=app_configs.get("postscript", {}),
                    logging_file=logging_file
                )
            )

        [f.result() for f in analysis]
        dfk.wait_for_current_tasks()
        logger.info("Workflow complete")

    # Create the Workflow Gantt chart
    logs = parse_logdir("logs")

    success = [l for l in logs if l.success]
    failed = [l for l in logs if not l.success]
    print(f"N success: {len(success)}")
    print(f"N fail: {len(fail)}")
            
    with open("failed_runs.list", "w") as f:
        for l in fail:
            f.write(l.name)
            f.write("\n")
        
    with open("success_runs.list", "w") as f:
        for l in success:
            f.write(l.name)
            f.write("\n")

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("Matplotlib not installed, skipping creating "
                       "workflow Gantt chart")
    else:    
        fig, ax = plt.subplots(figsize=(15, 15))
        ax = plot_campaign(
            ax,
            logs,
            relative_to_launch=True,
            units="hour",
            name_pos="right+column"
        )
        plt.tight_layout()
        plt.savefig("exec_gantt.png")
    finally:
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
