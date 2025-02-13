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

from astropy.table import Table


# "esci_48_8cpus" "astro_48_8cpus"
@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "ckpt_96gb_8cpus"]),
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
    res_file : `File`
        Parsl File object pointing to the Results file associated with the image collection.

    Outputs
    -------
    workunit_path : `File`
        Parsl File object poiting to the resampled WorkUnit.
    """
    import numpy as np
    from astropy.table import Table
    from astropy.wcs import WCS

    from kbmod import ImageCollection
    from kbmod.configuration import SearchConfiguration
    import kbmod.reprojection as reprojection

    from lsst.daf.butler import Butler

    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("workflow.step1", logging_file.filepath)

    with ErrorLogger(logger):
        logger.info("Starting step 1.")

        ic_filename = inputs[0].filename
        ic_file = inputs[0].filepath

        pg_name = ic_filename.split(".collection")[0]
        meas_path = f"uncert_meas/{pg_name}.meas"
        if os.path.exists(meas_path):
            if not os.path.exists(outputs[0].filepath):
                # touch resampled.wu file so that Parsl
                # understands its' been cached.
                open(outputs[0].filepath, 'a').close()
            logger.info("Finished step 1. Measurements exist.")
            return outputs

        if os.path.exists(outputs[0].filepath):
            logger.info("Finished step 1. Resampled WU exists.")
            return outputs
        
        # Unravell inputs
        repo_root = runtime_config["butler_config_filepath"]
        search_conf_path = runtime_config.get("search_config_filepath", None)
        ic_file = inputs[0].filepath

        ####
        #    Run core tasks
        ###
        ic = ImageCollection.read(ic_file)
        ic.data.sort("mjd_mid")
        search_conf = SearchConfiguration.from_file(search_conf_path)

        # The "optimal" WCS is the one we used in the initial search
        # So pick that up from the results:
        results = Table.read(inputs[1].filepath)
        opt_wcs = WCS(json.loads(results.meta["wcs"]))

        butler = Butler(repo_root)
        wu = ic.toWorkUnit(search_config=search_conf, butler=butler)
        del ic  # we're done with IC and results
        del results  # clean them up for memory

        resampled_wu = reprojection.reproject_work_unit(
            wu,
            opt_wcs,
            parallelize=True,
            max_parallel_processes=runtime_config.get("n_workers", 8),
        )
        resampled_wu.to_fits(outputs[0].filepath, overwrite=True)

    logger.info("Finished step 1.")
    return outputs


# "esci_48_8cpus" "astro_48_8cpus"
@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "esci_32gb_2cpu_1gpu"]),
    ignore_for_cache=["logging_file"],
)
def step2(inputs=(), outputs=(), runtime_config={}, logging_file=None):
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
    wu_file : `File`
        Parsl File object pointing to the WorkUnit.
    ic_file : `File`
        Parsl File object poiting to the associated ImageCollection.
    res_file : `File`
        Parsl File object poiting to the associated ImageCollection.
    uuids : `list`
        List of UUID hex representations corresponding to results we want to
        measure uncertainties for.

    Outputs
    -------
    workunit_path : `File`
        Parsl File object poiting to the resampled WorkUnit.
    """    
    import json

    import numpy as np
    import astropy.units as u
    from astropy.table import Table
    
    import lsst.daf.butler as dafButler

    from kbmod.work_unit import WorkUnit
    from kbmod.trajectory_explorer import TrajectoryExplorer

    from kbmod_wf.task_impls import calc_skypos_uncerts
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("workflow.step2", logging_file.filepath)

    with ErrorLogger(logger):
        logger.info("Starting step 2.")

        if os.path.exists(outputs[0].filepath):
            logger.info("Finished step 2. Measurements exist.")
            return outputs
        
        wu_path = inputs[0][0].filepath
        coll_path = inputs[1].filepath
        res_path = inputs[2].filepath
        uuids = inputs[3]

        # Run the search
        wu = WorkUnit.from_fits(wu_path)
        results = Table.read(res_path)
        explorer = TrajectoryExplorer(wu.im_stack)

        mjds = results.meta["mjd_mid"]
        mjd_start = np.min(mjds)
        mjd_end = np.max(mjds)

        wcs = wu.wcs

        uuids2, pgs, startt, endt = [], [], [], []
        p1ra, p1dec, sigma_p1ra, sigma_p1dec = [], [], [], []
        p2ra, p2dec, sigma_p2ra, sigma_p2dec = [], [], [], []
        likelihoods, uncerts = [], []
        for uuid in uuids:
            r = results[results["uuid"] == uuid]
            samples = explorer.evaluate_around_linear_trajectory(
                r["x"][0],
                r["y"][0],
                r["vx"][0], 
                r["vy"][0],
                pixel_radius=10,
                max_ang_offset=0.785397999997775,  # np.pi/4
                ang_step=1.5*0.0174533,  # deg2rad
                max_vel_offset=45,
                vel_step=0.55,
            )

            maxl = samples["likelihood"].max()
            bestfit = samples[samples["likelihood"] == maxl]
            # happens when oversampling
            if len(bestfit) > 1:
                bestfit = bestfit[:1]

            start_coord, end_coord, uncert = calc_skypos_uncerts(
                samples,
                mjd_start,
                mjd_end,
                wcs
            )

            uuids2.append(uuid)
            startt.append(mjd_start)
            endt.append(mjd_end)
            likelihoods.append(maxl)
            p1ra.append(start_coord.ra.deg)
            p1dec.append(start_coord.dec.deg)
            p2ra.append(end_coord.ra.deg)
            p2dec.append(end_coord.dec.deg)
            sigma_p1ra.append(uncert[0,0])
            sigma_p1dec.append(uncert[1,1])
            sigma_p2ra.append(uncert[2,2])
            sigma_p2dec.append(uncert[3,3])
            uncerts.append(uncert)

        t = Table({
            "likelihood": likelihoods,
            "p1ra": p1ra,
            "p1dec": p1dec,
            "p2ra": p2ra,
            "p2dec": p2dec,
            "sigma_p1ra": np.sqrt(sigma_p1ra),
            "sigma_p1dec": np.sqrt(sigma_p1dec),
            "sigma_p2ra": np.sqrt(sigma_p2ra),
            "sigma_p2dec": np.sqrt(sigma_p2dec),
            "uncertainty": uncerts,
            "uuid": uuids2,
            "t0": startt,
            "t1": endt
        })
        t.write(outputs[0].filepath, format="ascii.ecsv", overwrite=True)
        logger.info("Finished step 2.")

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

        result_ic_lookup = Table.read("resources/uuid-pg-lookup.ecsv")
        result_ic_lookup = result_ic_lookup.group_by("pg")

        # bookeping, used to build future output filenames
        resfiles, collfiles, uuids_per_pg, collnames, resampled_wus = [], [], [], [], []
        for g in result_ic_lookup.groups:
            resfname = g["pg"][0]
            results_file = File(f"results/{resfname}")
            resfiles.append(results_file)
            
            collname = resfname.replace(".results.ecsv", "")
            collnames.append(collname)
            
            collection = f"{collname}.collection"
            collection_file = File(os.path.join(directory_path, collection))
            collfiles.append(collection_file)
            uuids_per_pg.append(list(g["uuid"]))

            logger.info(f"Registering {collname} for step1 of {collection}")
            logging_file = File(f"logs/{collname}.resample.log")
            
            resampled_wus.append(
                step1(
                    inputs=[collection_file, results_file],
                    outputs=[File(f"resampled_wus/{collname}.resampled.wu")],
                    runtime_config=app_configs.get("step1", {}),
                    logging_file=logging_file,
                    )
                )

        results = []
        for resampledwu, collname, collfile, resfile, uuids in zip(resampled_wus, collnames, collfiles, resfiles, uuids_per_pg):
            logger.info(f"Registering {collname} for step2 of {collfile.filepath}")
            logging_file = File(f"logs/{collname}.search.log")

            results.append(
                step2(
                    inputs=[resampledwu, collfile, resfile, uuids],
                    outputs=[File(f"uncert_meas/{collname}.meas"),],
                    runtime_config=app_configs.get("step2", {}),
                    logging_file=logging_file,
                )
            )

        [f.result() for f in results]
        dfk.wait_for_current_tasks()
        logger.info("Workflow complete")


    # Create the Workflow Gantt chart
    logs = parse_logdir("logs")

    success, fail = [], []
    for l in logs:
        successfull_steps = l.stepnames[l.success]
        if not all([
                "resample" in successfull_steps,
                "search" in successfull_steps,
                "analysis" in successfull_steps
        ]):
            fail.append(l)
        else:
            success.append(l)

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









