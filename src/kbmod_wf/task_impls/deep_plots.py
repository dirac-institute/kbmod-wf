import dataclasses
import json

import matplotlib.pyplot as plt
from matplotlib import gridspec
from matplotlib.gridspec import GridSpec

import numpy as np
from astropy.table import Table
from astropy.time import Time
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS


__all__ = [
    "Figure",
    "configure_plot",
    "plot_result",
    "result_to_skycoord",
    "select_known_objects",
    "plot_objects",
    "plot_result"
]


KNOWN_OBJECTS_PLTSTYLE = {
    "fakes": {
        "tno": {
            "color": "purple",
            "label": "Fake TNO",
            "linewidth": 1,
            "markersize": 2,
            "marker": "o",
            "start_marker": "^",
            "start_color": "green"
        },
        "asteroid": {
            "color": "red",
            "label": "Fake Asteroid",
            "linewidth": 1,
            "markersize": 2,
            "marker": "o",
            "start_marker": "^",
            "start_color": "green"
        }
    },
    "knowns": {
        "KBO": {
            "color": "darkorange",
            "label": "Known KBO",
            "linewidth": 1,
            "markersize": 2,
            "marker": "o",
            "start_marker": "^",
            "start_color": "green"
        },
        "*": {
            "color": "chocolate",
            "label": "Known object",
            "linewidth": 1,
            "markersize": 2,
            "marker": "o",
            "start_marker": "^",
            "start_color": "green"
        }
    }
}
"""Default plot style for known objects."""


@dataclasses.dataclass
class Figure:
    """Figure area containing Axes named ``likelihood``, ``sky``,
    ``stamps`` and ``normed_stamps`` and ``psiphi`` axis, twinned to
    ``likelihood``.

    The class does not define a layout, nor data, for these axes,
    just their content.
    """
    fig: plt.Figure
    stamps: list[plt.Axes]
    normed_stamps: plt.Axes
    likelihood: plt.Axes
    psiphi: plt.Axes
    sky: plt.Axes


def configure_plot(
        wcs,
        fig_kwargs=None,
        gs_kwargs=None,
        layout="tight"
):
    """Configure a `Figure` and place `Axes` within that figure.

    The returned plot area is a 2x2 layout, with axes named
    ``likelihood``, ``sky``, ``stamps`` and ``normed_stamps``, going
    in clockwise direction. The top left axis has a twinned y axis
    named ``psiphi``. The stamps are 1x4 Axes with no axis labels or
    ticks.

    This function only provides this layout and it does not plot data.
 
    Parameters
    ----------
    wcs : `WCS`
        WCS class to added to ``sky`` axis.
    fig_kwargs : `dict` or `None`, optional
        Keyword arguments passed forwards to `plt.figure`.
    gs_kwargs : `dict` or `None`, optional
        Keyword arguments passed forwards to `GridSpec`.
    layout: `str`, optional
        Figure layout is by default ``tight``.

    Returns
    -------
    Figure : `obj`
       Dataclass containing all of the created Axes, see `Figure`
    """
    fig_kwargs = {} if fig_kwargs is None else fig_kwargs
    lk = fig_kwargs.pop("layout", None)
    layout = "tight" if lk is None else lk
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


def result_to_skycoord(result, times, obs_valid, wcs):
    """Return a collection of on-sky coordinates that match the result.

    Take a result entry and return its SkyCoord positions on the sky.

    Parameters
    ----------
    results : `Row`
        Result
    times : `np.array`
        Array of MJD timestamps as floats.
    obs_valid : `list[bool]`
        A list of which observations are valid.
    wcs : `WCS`
        WCS

    Returns
    -------
    coords : `SkyCoord`
        World coordinates.
    pos_valid : `list[bool]`
        List of valid observations.
    """
    pos, pos_valid = [], []
    times = Time(times, format="mjd")
    dt = (times - times[0]).value

    newx = result["x"]+dt*result["vx"]
    newy = result["y"]+dt*result["vy"]
    coord = wcs.pixel_to_world(newx, newy)
    #pos.append(list(zip(coord.ra.deg, coord.dec.deg)))
    #pos_valid.append(obs_valid)                        # NOTE TO SELF: FIX LATER

    return coord, obs_valid #SkyCoord(pos), pos_valid


def select_known_objects(fakes, known_objs, results):
    """Select known objects and known inserted fake objects.

    Parameters
    ----------
    fakes : `Table`
        Table containing visit and detector columns to match on.
    known_objs : `Table`
        SkyBot results containing ephemeris of all known objects at
        the same timestamps.
    results: `Results`
        Results

    Returns
    -------
    fakes : `Table`
        Filtered ingoing table of fakes.
    knowns : `Table`
        Filetered ingoing table of knwon objects.
    """
    visitids = results.meta["visits"]
    detector = results.meta["detector"]
    obstimes = results.meta["mjd_mid"]
    wcs = WCS(json.loads(results.meta["wcs"]))

    mask = fakes[1].data["CCDNUM"] == detector
    visitmask = fakes[1].data["EXPNUM"][mask] == visitids[0]
    for vid in visitids[1:]:
        visitmask = np.logical_or(
            visitmask,
            fakes[1].data["EXPNUM"][mask] == vid
        )
    fakes = Table(fakes[1].data[mask][visitmask])
    fakes = fakes.group_by("ORBITID")

    (blra, bldec), (tlra, tldec), (trra, trdec), (brra, brdec) = wcs.calc_footprint()
    padding = 0.005
    mask = (
        (known_objs["RA"] > tlra-padding) &
        (known_objs["RA"] < blra+padding) &
        (known_objs["DEC"] > bldec-padding) &
        (known_objs["DEC"] < trdec+padding)
    )
    knowns = known_objs[mask].group_by("Name")

    return fakes, knowns


def plot_objects(ax, objs, type_key, plot_kwargs, sort_on="mjd_mid"):
    """Plots objects onto the given WCSAxes.

    Objects are a table containing ``RA``, ``DEC``, `type_key` and
    `sort_on` columns, grouped by the individual object. The object
    positions are plotted as a scattered plot, with each object getting
    a different visual formatting based on the object type value.
    The `type_key` names a column that selects the visual formatting
    via `plot_kwargs`. The plot keyword arguments must match the name
    of the type of the object, f.e. "tno", "kbo", "asteroid" etc. or
    the ``"*"`` literal to match any not-specified object type.
    
    The `plot_kwargs` is a dictionary with keys matching the desired
    object type. Value of each key is a dictionary with appropriate
    key-value pairs passed onto `ax.plot_coord`. Additionally, the
    dictionary may contain keys ``start_marker`` and ``start_color``
    keys that will be used to differently visualize the first position
    of the object on the plot, to visualize the direction of motion.
    By default this is a green triangle.

    Parameters
    ----------
    ax : `WCSAxes`
        Axis
    objs : `Table`
        Catalog of object positions, grouped by individual object.
    type_key : `str`
        Name of the column that contains the object type, f.e. ``tno``,
        ``kbo``, ``asteroid`` etc.
    plot_kwargs : `dict`
        Dictionary containing the names of the object types, or ``*``,
        and their formatting parameters. Optionally ``start_marker``
        and ``start_color`` may be provided for each object class to
        mark the first object position.
    sort_on : `str`, optional
        Name of the column on which to sort each object on. By default
        ``mjd_mid``.

    Returns
    -------
    ax : `WCSAxes`
        Axis containing the artists.
    """
    plt_kwargs = plot_kwargs.copy()
    legend_entries = []
    for group in objs.groups:
        if sort_on is not None:
            group.sort(sort_on)
                
        kind = np.unique(group[type_key])
        if len(kind) > 1:
            raise ValueError(
                "Object can only be classified into a single type. "
                f"Got {kind} instead"
            )

        obj_type = group[type_key][0]
        if obj_type not in plt_kwargs.keys():
            if "*" in plt_kwargs.keys():
                obj_type = "*"
            else:
                raise ValueError(
                    f"Object type `{obj_type}` not found in the plot "
                    f"arguments `{plt_kwargs.keys()}`"
                )

        sm = plt_kwargs[obj_type].pop("start_marker", "^")
        sms = plt_kwargs[obj_type].pop("start_markersize", 1)
        sc = plt_kwargs[obj_type].pop("start_color", "green")
        pos = SkyCoord(group["RA"], group["DEC"], unit="degree",
                       frame="icrs")

        ax.plot_coord(pos, **plt_kwargs[obj_type])
        ax.scatter_coord(pos[0], marker=sm, color=sc)

    ax.legend(legend_entries)
    return ax


def plot_result(figure, res, fakes, knowns, wcs, obstimes):
    """Plot a single result onto the `Figure`.

    Four axes are plotted for each result, the likelihood and psi and
    phi, the footprint of the WCS with positions of the result and
    known objects within it, and two sets of postage stamp cutouts
    centered on the results positions. One shares the normalization
    range and the other does not.

    Parameters
    ----------
    figure : `Figure`
        Dataclass containing all the axes of the plot.
    res : `Row`
        Result.
    fakes : `Table`
        Catalog of all simulated objects positions. Must contain ``RA``
        ``DEC``, ``mjd_mid`` and ``type`` columns. Must be grouped by
         individual object.
    knowns : `Table`
        Catalog of all known real objects. Must contain ``RA``, ``DEC``
        ``Type`` and ``mjd_mid`` columns. Must be grouped by individual
        object.

    Returns
    -------
    figure : `Figure`
        Figure containing all the axes and their artists.
    """
    # Top Left plot
    #    - Phi, Psi and Likelihood values
    figure.psiphi.plot(res["psi_curve"], alpha=0.25, marker="o", label="psi")
    figure.psiphi.plot(res["phi_curve"], alpha=0.25, marker="o", label="phi")
    figure.psiphi.legend(loc="upper right")

    figure.likelihood.plot(res["psi_curve"]/res["phi_curve"], marker="o", label="L", color="red")
    figure.likelihood.set_title(
        f"Likelihood: {res['likelihood']:.5}, obs_count: {res['obs_count']}, \n "
        f"(x, y): ({res['x']}, {res['y']}), (vx, vy): ({res['vx']:.6}, {res['vy']:.6})"
    )
    figure.likelihood.legend(loc="upper left")

    # Top right
    #    - footprint of the CCD
    #    - known fake objects
    #    - known real objects
    #    - trajectory of the result
    # Order is important because of the z-level of the plotted artists
    (blra, bldec), (tlra, tldec), (trra, trdec), (brra, brdec) = wcs.calc_footprint()
    figure.sky.plot(
        [blra, tlra, trra, brra, blra],
        [bldec, tldec, trdec, brdec, bldec],
        transform=figure.sky.get_transform("world"),
        color="black", label="Footprint"
    )

    if len(fakes) > 0:
        figure.sky = plot_objects(figure.sky, fakes, "type", KNOWN_OBJECTS_PLTSTYLE["fakes"])
        
    if len(knowns) > 0:
        figure.sky = plot_objects(figure.sky, knowns, "Type", KNOWN_OBJECTS_PLTSTYLE["knowns"])

    pos, pos_valid = result_to_skycoord(res, obstimes, res["obs_valid"], wcs)
    figure.sky.plot_coord(pos, marker="o", markersize=1, linewidth=1, label="Search trj.", color="C0")        
    if sum(pos_valid) > 0:
        figure.sky.scatter_coord(pos[pos_valid], marker="+", alpha=0.25, label="Obs. valid", color="C0")
    figure.sky.scatter_coord(pos[0], marker="^", color="green", label="Starting point")

    # de-duplicate the axis entries
    bb = {name : handle for handle, name in zip(*figure.sky.get_legend_handles_labels())}
    handles = list(bb.values())
    names = list(bb.keys())
    figure.sky.legend(bb.values(), bb.keys(), loc="upper left", ncols=7)

    # Bottom left
    #    - individually scaled coadd stamps
    stamp_types = ("coadd_mean", "coadd_median",
                   "coadd_weighted", "coadd_sum")
    for ax, kind in zip(figure.stamps.ravel(), stamp_types):
        ax.imshow(res[kind], interpolation="none")
        ax.set_title(kind)

    # Bottom right
    #    - postage stamps scaled to the mean stamp
    ntype = stamp_types[0]
    for ax, kind in zip(figure.normed_stamps.ravel(), stamp_types):
        ax.imshow(res[kind], vmin=res[ntype].min(),
                  vmax=res[ntype].max(), interpolation="none")
        ax.set_title(kind)

    return figure
