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

@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "gpu"]), # TODO verify if gpu is needed
    ignore_for_cache=["logging_file"],
)
def get_uncertainties(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    """Loads a WorkUnit and KBMOD results and calculates the uncertaintties for those results 

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
    res_file : `File`
        KBMOD results file corresponding to this WorkUnit.
    uuids : `list`
        List of UUID hex representations corresponding to results we want to
        measure uncertainties for. If empty, all results will be used.

    Outputs
    -------
    workunit_path : `File`
        Parsl File object poiting to the resampled WorkUnit.
    """    
    import json

    import numpy as np
    from astropy.table import Table

    from kbmod.work_unit import WorkUnit
    from kbmod.trajectory_explorer import TrajectoryExplorer

    from kbmod_wf.task_impls import calc_skypos_uncerts
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("workflow.get_uncertainties", logging_file.filepath)

    with ErrorLogger(logger):
        logger.info("Starting getting uncertainties.")

        if os.path.exists(outputs[0].filepath):
            logger.info("Finished step 2. Uncertainties exist.")
            return outputs
        
        wu_path = inputs[0].filepath
        res_path = inputs[1].filepath
        uuids = inputs[2]

        # Load the WorkUnit
        wu = None
        try:
            wu = WorkUnit.from_fits(wu_path)
            logger.info(f"Loaded WorkUnit from fits")
        except Exception as e:
            wu_filename = os.path.basename(wu_path)
            wu_dir = os.path.dirname(wu_path)
            wu = WorkUnit.from_sharded_fits(wu_filename, wu_dir, lazy=False)
            logger.info(f"Loaded WorkUnit from sharded fits")

        # Load results from this WorkUnit
        results = Table.read(res_path)
        explorer = TrajectoryExplorer(wu.im_stack)

        wcs = wu.wcs

        uuids2, pgs, startt, endt = [], [], [], []
        p1ra, p1dec, sigma_p1ra, sigma_p1dec = [], [], [], []
        p2ra, p2dec, sigma_p2ra, sigma_p2dec = [], [], [], []
        likelihoods, uncerts = [], []

        if len(uuids) > 0:
            # If the user specified a list of UUIDs, filter the results to just those UUIDs
            results.table = results.table[results["uuid"].isin(uuids)]

        for r in results:
            # TODO Make UUIDs required
            uuid = r["uuid"] if "uuid" in r.table.colnames else -1
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

            # Get the valid obstimes that were used this result
            valid_obstimes = []
            for i in range(len(r["obs_valid"])):
                if r["obs_valid"][i]:
                    valid_obstimes.append(wu.im_stack.get_obstime(i))

            mjd_start = min(valid_obstimes)
            mjd_end = max(valid_obstimes)

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

        # Get filenames from runtime config
        wu_path = runtime_config.get("wu_path", None)
        wu_name = os.path.basename(wu_path)
        res_path = runtime_config.get("res_path", None)
        uuids = runtime_config.get("uuids", [])
        output_dir = runtime_config.get("output_dir", os.getcwd())

        # create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
      
        # run kbmod search on each reprojected WorkUnit
        uncertainty_future = get_uncertainties(
                inputs=[File(wu_path), File(res_path), uuids],
                outputs=[File(os.path.join(output_dir, f"{wu_name}.meas"))],
                runtime_config=app_configs.get("kbmod_search", {}),
                logging_file=logging_file,
            )
        uncertainty_future.result()
        dfk.wait_for_current_tasks()

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
