import logging
logging.basicConfig(
    level=logging.INFO,
    filename="kbmod.log",
    format="[%(asctime)s %(levelname)s %(name)s] %(message)s"
)
stdout = logging.StreamHandler()
stdout.setLevel(logging.INFO)
logging.getLogger("").addHandler(stdout)

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
    parse_logdir,
    plot_campaign
)


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "ckpt_48gb_8cpus"]), # "esci_48_8cpus" "astro_48_8cpus"
    ignore_for_cache=["logging_file"],
)
def step1(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("kbmod.ic_to_wu", logging_file.filepath)

    import numpy as np
    from reproject.mosaicking import find_optimal_celestial_wcs

    from kbmod import ImageCollection
    from kbmod.configuration import SearchConfiguration
    import kbmod.reprojection as reprojection

    from lsst.daf.butler import Butler

    with ErrorLogger(logger):
        # Unravell inputs
        repo_root = runtime_config["butler_config_filepath"]
        search_conf_path = runtime_config.get("search_config_filepath", None)
        ic_file = inputs[0].filepath

        ####
        #    Run core tasks
        ###
        # Load the image collection and add quality cuts on the data
        # Zeropoint cuts would be good, but it does then get confusing
        # because what is run is not the whole collection. The WCS
        # HAS to be cut, because resampling won't work correctly (not
        # an error, but scientifically invalid). This should never
        # happen because we already filter out the bad CCDs in the
        # workflow. Just in case though....
        ic = ImageCollection.read(ic_file)

        #mask_zp = np.logical_and(ic["zeroPoint"] > 27 , ic["zeroPoint"] < 32)
        mask_wcs_err = ic["wcs_err"] < 1e-04
        #ic = ic[np.logical_and(mask_zp, mask_wcs_err)]
        ic = ic[mask_wcs_err]
        ic.reset_lazy_loading_indices()
        ic.data.sort("mjd_mid")

        # Adjust the search parameters based on the selection
        # currently that's only the n_obs, but could be lh_threshold too
        search_conf = SearchConfiguration.from_file(search_conf_path)
        #n_obs = len(ic)//2 if len(ic)//2 > 40 else 40
        n_obs = len(ic)//2
        search_conf._params["n_obs"] = n_obs

        # Fit the optimal WCS
        # TODO: triple check this doesn't flip the array, I'm pretty sure it does
        opt_wcs, shape = find_optimal_celestial_wcs(list(ic.wcs))
        opt_wcs.array_shape = shape

        # Standardize the images, and put them in a WorkUnit
        butler = Butler(repo_root)
        wu = ic.toWorkUnit(search_config=search_conf, butler=butler)

        # we've got everything we wanted out of IC, clean it up.
        del ic

        # Resample the work unit so all pixels point to the same (ra, dec)
        resampled_wu = reprojection.reproject_work_unit(
            wu,
            opt_wcs,
            parallelize=True,
            max_parallel_processes=runtime_config.get("n_workers", 8),
        )
        resampled_wu.to_fits(outputs[0], overwrite=True)

    return outputs


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "ckpt_32gb_2cpu_1gpu"]), # "esci_48_2cpu_1gpu", "esci_48_2cpu_1gpu"
    ignore_for_cache=["logging_file"],
)
def step2(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("kbmod.search_task", logging_file.filepath)

    import json

    from kbmod import ImageCollection
    from kbmod.work_unit import WorkUnit
    from kbmod.run_search import SearchRunner

    with ErrorLogger(logger):
        wu_path = inputs[0][0].filepath
        coll_path = inputs[1].filepath

        ic = ImageCollection.read(coll_path)
        ic.data.sort("mjd_mid")
        wu = WorkUnit.from_fits(wu_path)
        res = SearchRunner().run_search_from_work_unit(wu)

        # a WCS in the results table would be very helpful
        # so add it in.
        header = wu.wcs.to_header(relax=True)
        header["NAXIS1"], header["NAXIS2"] = wu.wcs.pixel_shape
        res.table.meta["wcs"] = json.dumps(dict(header))
        res.table.meta["visits"] = list(ic["visit"].data)
        res.table.meta["detector"] = ic["detector"][0]
        res.table.meta["mjd_mid"] = list(ic["mjd_mid"].data)

        # write the results to a file
        res.write_table(outputs[0].filepath, overwrite=True)

    return outputs


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "ckpt_4gb_2cpus"]),#"ckpt_2gb_2cpus", "ckpt_2gb_2cpus", "astro_2gb_2cpus"]),
    ignore_for_cache=["logging_file"],
)
def postscript(inputs=(), outputs=(), runtime_config={}, logging_file=None):
    from kbmod_wf.utilities.logger_utilities import get_configured_logger, ErrorLogger
    logger = get_configured_logger("kbmod.analysis_task", logging_file.filepath)

    import dataclasses
    import tempfile
    import tarfile
    import shutil

    import matplotlib.pyplot as plt
    from matplotlib import gridspec
    from matplotlib.gridspec import GridSpec

    import numpy as np
    import astropy.io.fits as fitsio
    from astropy.table import Table
    from astropy.time import Time
    from astropy.coordinates import SkyCoord
    from astropy.wcs import WCS

    @dataclasses.dataclass
    class Figure:
        fig: plt.Figure
        stamps: list[plt.Axes]
        normed_stamps: plt.Axes
        likelihood: plt.Axes
        psiphi: plt.Axes
        sky: plt.Axes

    def configure_plot(wcs, fig_kwargs=None, gs_kwargs=None, height_ratios=[1, 1], width_ration=[1, 1], layout="tight"):
        fig_kwargs = {} if fig_kwargs is None else fig_kwargs
        gs_kwargs = {} if gs_kwargs is None else gs_kwargs

        fig = plt.figure(layout=layout, **fig_kwargs)

        fig_gs = GridSpec(2, 2, figure=fig,  **gs_kwargs)
        stamp_gs = gridspec.GridSpecFromSubplotSpec(1, 4, hspace=0.01, wspace=0.01, subplot_spec=fig_gs[1, 0])
        stamp_gs2 = gridspec.GridSpecFromSubplotSpec(1, 4, hspace=0.01, wspace=0.01, subplot_spec=fig_gs[1, 1])

        ax_left = fig.add_subplot(stamp_gs[:])
        ax_left.axis('off')
        ax_left.set_title('Coadded cutouts')

        ax_right = fig.add_subplot(stamp_gs2[:])
        ax_right.axis('off')
        ax_right.set_title('Coadded cutouts normalized to mean values.')

        stamps = np.array([fig.add_subplot(stamp_gs[i]) for i in range(4)])
        
        for ax in stamps[1:]:
            ax.sharey(stamps[0])
            plt.setp(ax.get_yticklabels(), visible=False)

        normed = np.array([fig.add_subplot(stamp_gs2[i]) for i in range(4)])
        for ax in normed[1:]:
            ax.sharey(normed[0])
            plt.setp(ax.get_yticklabels(), visible=False)

        likelihood = fig.add_subplot(fig_gs[0, 0])
        psiphi = likelihood.twinx()
        likelihood.set_ylabel("Likelihood")
        psiphi.set_ylabel("Psi, Phi value")
        likelihood.set_xlabel("i-th image in stack")

        sky = fig.add_subplot(fig_gs[0, 1], projection=wcs)
        overlay = sky.get_coords_overlay('geocentricmeanecliptic')
        overlay.grid(color='black', ls='dotted')
        sky.coords[0].set_major_formatter('d.dd')
        sky.coords[1].set_major_formatter('d.dd')

        return Figure(fig, stamps, normed, likelihood, psiphi, sky)

    def result_to_skycoord(res, times, obs_valid, wcs):
        pos, pos_valid = [], []
        times = Time(times, format="mjd")
        dt = (times - times[0]).value
        for i in range(len(obs_valid)):
            newx, newy = res["x"]+i*dt[i]*res["vx"], res["y"]+i*dt[i]*res["vy"]
            if newx < 0 or newy < 0:
                continue
            if newx > wcs.pixel_shape[0] or newy > wcs.pixel_shape[1]:
                continue
            pos.append(wcs.pixel_to_world(newx, newy))
            pos_valid.append(obs_valid[i])
        return SkyCoord(pos), pos_valid

    with ErrorLogger(logger):
        results_path = inputs[0][0].filepath
        collname = os.path.basename(results_path).split(".")[0]
        results = Table.read(results_path)

        if len(results) == 0:
            logger.info(f"No results found in {results_path}")
            return

        tmpdir = tempfile.mkdtemp()
        fakes = fitsio.open("/gscratch/dirac/dinob/workflows/resources/fakes_detections_joined.minified.fits.bz2")
        allknowns = Table.read("/gscratch/dirac/dinob/workflows/resources/skybot_results_joined.minified.fits.bz2")
        visitids = results.meta["visits"]
        detector = results.meta["detector"]
        obstimes = results.meta["mjd_mid"]
        wcs = WCS(json.loads(results.meta["wcs"]))

        mask = fakes[1].data["CCDNUM"] == detector
        visitmask = fakes[1].data["EXPNUM"][mask] == visitids[0]
        for vid in visitids[1:]:
            visitmask = np.logical_or(visitmask, fakes[1].data["EXPNUM"][mask] == vid)
        fakes = Table(fakes[1].data[mask][visitmask])
        fakes = fakes.group_by("ORBITID")

        (blra, bldec), (tlra, tldec), (trra, trdec), (brra, brdec) = wcs.calc_footprint()
        padding = 0.005
        mask = (allknowns["RA"] > tlra-padding) & (allknowns["RA"] < blra+padding) & (allknowns["DEC"] > bldec-padding) & (allknowns["DEC"] < trdec+padding)
        knowns = allknowns[mask].group_by("Name")

        allplots = []
        logger.info(f"Creating analysis plots for results of length: {len(results)}")
        for i, res in enumerate(results):
            figure = configure_plot(wcs, fig_kwargs={"figsize": (24, 12)})
            figure.fig.suptitle(f"{collname}, idx: {i}")

            if len(fakes) > 1:
                set_ast_lbl, set_tno_lbl = False, False
                for group in fakes.groups:
                    group.sort("mjd_mid")
                    kind = np.unique(group["type"])
                    if len(kind) > 1:
                        logger.error("More than 1 kind, shouldn't happen!")
                    if group["type"][0] == "tno":
                        color = "purple"
                        lbl = "Fake TNO" if not set_tno_lbl else None
                        set_tno_lbl = True
                    if group["type"][0] == "asteroid":
                        color = "red"
                        lbl = "Fake Asteroid" if not set_ast_lbl else None
                        set_ast_lbl = True
                    pos = SkyCoord(group["RA"], group["DEC"], unit="degree", frame="icrs")
                    figure.sky.plot_coord(pos, marker="o", markersize=2, linewidth=1, color=color, label=lbl)
                    figure.sky.scatter_coord(pos[0], marker="^", color="green")

            if len(knowns) > 1:
                set_ast_lbl, set_tno_lbl = False, False
                for group in knowns.groups:
                    group.sort("mjd_mid")
                    kind = np.unique(group["Type"])
                    if "KBO" in group["Type"][0]:
                        color = "darkorange"
                        lbl = "Known KBO" if not set_tno_lbl else None
                        set_tno_lbl = True
                    else:
                        color = "chocolate"
                        lbl = "Known Asteroid" if not set_ast_lbl else None
                        set_ast_lbl = True
                    pos = SkyCoord(group["RA"], group["DEC"], unit="degree", frame="icrs")
                    figure.sky.plot_coord(pos, marker="o", markersize=2, linewidth=1, color=color, label=lbl)
                    figure.sky.scatter_coord(pos[0], marker="^", color="green")

            pos, pos_valid = result_to_skycoord(res, obstimes, res["obs_valid"], wcs)
            figure.sky.plot_coord(pos, marker="o", markersize=1, linewidth=1, label="Search trj.", color="C0")
            figure.sky.scatter_coord(pos[0], marker="^", color="green", label="Starting point")
            if sum(pos_valid) > 0:
                figure.sky.scatter_coord(pos[pos_valid], marker="+", alpha=0.25, label="Obs. valid", color="C0")
            figure.sky.plot(
                [blra, tlra, trra, brra, blra], [bldec, tldec, trdec, brdec, bldec],
                transform=figure.sky.get_transform("world"),
                color="black", label="Footprint"
            )
            figure.sky.legend(loc="upper left", ncols=7)

            stamp_types = ("coadd_mean", "coadd_median", "coadd_weighted", "coadd_sum")
            ntype = stamp_types[0]
            for ax, kind in zip(figure.stamps.ravel(), stamp_types):
                ax.imshow(res[kind], interpolation="none")
                ax.set_title(kind)
            for ax, kind in zip(figure.normed_stamps.ravel(), stamp_types):
                ax.imshow(res[kind], vmin=res[ntype].min(), vmax=res[ntype].max(), interpolation="none")
                ax.set_title(kind)

            figure.psiphi.plot(res["psi_curve"], alpha=0.25, marker="o", label="psi")
            figure.psiphi.plot(res["phi_curve"], alpha=0.25, marker="o", label="phi")
            figure.psiphi.legend(loc="upper right")

            figure.likelihood.plot(res["psi_curve"]/res["phi_curve"], marker="o", label="L", color="red")
            figure.likelihood.set_title(
                f"Likelihood: {res['likelihood']:.5}, obs_count: {res['obs_count']}, \n "
                f"(x, y): ({res['x']}, {res['y']}), (vx, vy): ({res['vx']:.6}, {res['vy']:.6})"
            )
            figure.likelihood.legend(loc="upper left")

            pltname = f"{collname}_L{int(res['likelihood']):0>4}_idx{i:0>4}.jpg"
            pltpath = os.path.join(tmpdir, pltname)
            allplots.append(pltpath)
            logger.info(f"Saving {pltpath}")
            plt.savefig(pltpath)
            plt.close(figure.fig)
            
        with tarfile.open(outputs[0], "w|bz2") as tar:
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

        logger.info("Starting workflow")
        directory_path = runtime_config.get("staging_directory", "collections")
        file_pattern = runtime_config.get("file_pattern", "*.collection")
        pattern = os.path.join(directory_path, file_pattern)
        entries = glob.glob(pattern)
        logger.info(f"Found {len(entries)} files in {directory_path}")

        # bookeping, used to build future output filenames
        collnames = []
        collfiles = []
        resampled_wus = []
        for collection in entries:
            # skip detectors 31, 61 and 2
            if "002" in collection or "031" in collection or "061" in collection:
                print(f"Skipping {collection} bad detector.")
                continue

            # bookeeping for future tasks
            collname = os.path.basename(collection)
            collname = collname.split(".")[0]
            collnames.append(collname)

            # task themselvves
            logger.info(f"Registering {collname} for step1 of {collection}")
            collection_file = File(collection)
            collfiles.append(collection_file)
            logging_file = File(f"logs/{collname}.resample.log")
            resampled_wus.append(
                step1(
                    inputs=[collection_file],
                    outputs=[File(f"resampled_wus/{collname}.resampled.wu")],
                    runtime_config=app_configs.get("ic_to_wu", {}),
                    logging_file=logging_file,
                )
            )

        results = []
        for resample, collname, collfile in zip(resampled_wus, collnames, collfiles):
            logger.info(f"Registering {collname} for step2 of {collfile.filepath}")
            logging_file = File(f"logs/{collname}.search.log")
            results.append(
                step2(
                    inputs=[resample, collfile],
                    outputs=[File(f"results/{collname}.results.ecsv"),],
                    runtime_config=app_configs.get("kbmod_search", {}),
                    logging_file=logging_file,
                )
            )

        analysis = []
        for result, collname in zip(results, collnames):
            logger.info(f"Registering {collname} for step3")
            logging_file = File(f"logs/{collname}.analysis.log")
            plots_archive = File(f"plots/{collname}.plots.tar.bz2")
            analysis.append(
                postscript(
                    inputs=[result],
                    outputs=[plots_archive, ],
                    runtime_config=app_configs.get("analysis", {}),
                    logging_file=logging_file
                )
            )
            
        [f.result() for f in analysis]
        dfk.wait_for_current_tasks()
        logger.info("Workflow complete")

    import matplotlib.pyplot as plt
    logs = parse_logdir("logs")
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
