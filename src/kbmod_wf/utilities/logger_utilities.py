import time
import traceback
import logging
from logging import config

import os
import re
import glob

import numpy as np
import matplotlib.pyplot as plt
from astropy.table import Table, vstack
from astropy.time import Time, TimeDelta


__all__ = [
    "LOGGING_CONFIG",
    "get_configured_logger",
    "ErrorLogger",
    "Log",
    "parse_logfile",
    "parse_logdir",
    "plot_campaign"
]


LOGGING_CONFIG = {
    "version": 1.0,
    "formatters": {
        "standard": {
            "format": (
                "[%(processName)s-%(process)d %(threadName)s-%(thread)d "
                "%(asctime)s %(levelname)s %(name)s] %(message)s"
            ),
        },
    },
    "handlers": {
        "stdout": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "stderr": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "file": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.FileHandler",
            "filename": "kbmod.log",
        },
    },
    "loggers": {
        "parsl": {"level": "INFO", "handlers": ["file", "stdout"], "propagate": False},
        "workflow": {"level": "INFO", "handlers": ["file", "stdout"], "propagate": False},
        "kbmod": {"level": "DEBUG", "handlers": ["file", "stdout"], "propagate": False},
    },
}
"""Default logging configuration for Parsl."""


def get_configured_logger(logger_name, file_path=None):
    """Configure logging to output to the given file.

    Parameters
    ----------
    logger_name : `str`
        Name of the created logger instance.
    file_path : `str` or `None`, optional
        Path to the log file, if any
    """
    logconf = LOGGING_CONFIG.copy()
    if file_path is not None:
        logconf["handlers"]["file"]["filename"] = file_path
    config.dictConfig(logconf)
    logging.Formatter.converter = time.gmtime
    logger = logging.getLogger()
    return logging.getLogger(logger_name)


class ErrorLogger:
    """Context manager that logs received errors before re-raising
    them.

    Parameters
    ----------
    logger : `logging.Logger`
        Logger instance that will be used to log the error.
    silence_errors : `bool`, optional
        Errors are not silenced by default but re-raised.
        Set this to `True` to silence errors.
    """

    def __init__(self, logger, silence_errors=False):
        self.logger = logger
        self.silence_errors = silence_errors

    def __enter__(self):
        return self

    def __exit__(self, exc, value, tb):
        if exc is not None:
            msg = traceback.format_exception(exc, value, tb)
            msg = "".join(msg)
            self.logger.error(msg)
            return self.silence_errors


class Log:
    """A log of a task.

    Note that each Log can consist of multiple log files. For example
    the complete log of a Task ``20210101_A`` can consist of multiple
    steps called ``20210101_A_step1``, ``20210101_A_step2`` etc. thus
    making the full Log of the event a union of every step.

    Individual steps are parsed, extracting individual log entries and
    context; assigning to each log step a success, error and completion
    status.
    A log is succesfull if all individual steps were succesfull.
    A step is not completed if the log does not contain the expected
    final log entry.
    A step is marked with an error if the log produces a traceback or
    an error report. A not completed log step does not indicate a error.
    Not-completed, but not-errored, steps may be safely re-run.    
    A log is a failure if all steps were not completed or any of the
    steps has an error. A log may be safely re-run if no steps have an
    error, but not all steps have completed.

    Each step may contain repeated sequences of messages. When
    running a task with Parsl, and it is pre-empted or it fails,
    it can be resubmitted - leading to repeated collections of
    messages.

    To overwrite the default contextualization of the parsed logs
    override the `_parse_single` method.
    
    Parameters
    ----------
    logfiles : `list`
        List of log files belonging to the same Log event.
    name : `str` or `None`, optional
        Name of the log event, f.e. ``20210101_A``. When not provided
        it will be determined as the longest shared common prefix of
        each step.
    stepnames : `list` or `None`, optional
        Name of the each step, f.e. ``20210101_A_step1`` etc. When not
        provided, it will be determined from the log itself.

    Attributes
    ----------
    logfiles : `list`
        Paths to log files.
    name : `str`
        Log group name
    stepnames : `list[str]`
        Names of steps involved in the log.
    nsteps : `int`
        Number of steps in the log.
    started : `list`
        List of step start times.
    completed : `list`
        List of step end times.
    success : `list[bool]`
        List of whether or not a step was determined to be succesfull.
    error : `bool`
        `True` when log contains a step with an error.
    data : `Table`
        Table of all log entries. Table is grouped by stepname and
        step index.
    """
    
    fmt: str = r"\[(?P<process>[\w|-]*) (?P<thread>[\w|-]*) (?P<time>[\d|\-| |:|,]+) (?P<level>\w*) (?P<logger>.*)] (?P<msg>.*)$"
    """Log line format."""

    def __init__(self, logfiles, name=None, stepnames=None):
        # set the name from filename if not provided
        if name is None:
            name = os.path.commonprefix(logfiles)
            if os.sep in name:
                name = os.path.basename(name)

        self.logfiles = logfiles
        self.name = name
        self.stepnames = [] if stepnames is None else stepnames
        self.nsteps = len(logfiles)
        self.started = []
        self.completed = []
        self.success = []

        tables = []
        for f in logfiles:
            log = parse_logfile(f)
            tbl = Table(log)
            tbl.sort("time")
            tbl = tbl.group_by(["process", "thread"])
            
            if len(tbl) == 0:
                continue
            
            tbl["stepname"] = "          "
            tbl["stepidx"] = 0
            for i, grp in enumerate(tbl.groups):
                started, completed, success, stepname = self._parse_single(f, grp)
                grp["stepname"] = stepname
                grp["stepidx"] = i
                self.stepnames.append(stepname)
                self.started.append(started)
                self.completed.append(completed)
                self.success.append(success)
            tables.append(tbl)

        self.stepnames = np.array(self.stepnames)
        self.started = np.array(self.started)
        self.completed = np.array(self.completed)
        self.success = np.array(self.success)
        
        self.data = vstack(tables)
        if len(self.data) > 0:
            self.data["time"] = Time([Time.strptime(t, "%Y-%m-%d %H:%M:%S,%f") for t in self.data["time"]])
            self.data.sort(["time", "stepidx"])
            self.error = not any(["Traceback" in msg for msg in self.data["msg"]])
            self.data = self.data.group_by(["stepname", "stepidx"])

    def _parse_single(self, name, table):
        """Contextualize each step in the log.

        Parameters
        ----------
        name : `str`
            Name of the log this step belongs to.
        table : `Table`
            Table of log entries of the step.

        Returns
        -------
        started : `bool`
            `True` if the step has any entries.
        completed : `bool`
            `True` if the step has the expected last message.
        success: `bool`
            `True` if the step has started, completed and has no errors.
        stepname: `str`
            Name of the step as deduced from the log entries.
        """
        started, completed, success = False, False, False
        stepname = ""
        
        if len(table) == 0:
            return started, completed, success

        firstmsg, lastmsg = table[0]["msg"], table[-1]["msg"]        
        if "resample" in name:
            if "Building WorkUnit" in firstmsg:
                started = True
            if "Writing" in lastmsg:
                completed = True
            error = any(["Traceback" in msg for msg in table["msg"]])
            success = not error and started and completed
            stepname = "resample"
        elif "search" in name:
            if "Loading WorkUnit from FITS file" in firstmsg:
                started = True
            if "Saving results to" in lastmsg:
                completed = True
            error = any(["Traceback" in msg for msg in table["msg"]])
            success = not error and started and completed
            stepname = "search"
        elif "analysis" in name:
            if "No results found" in firstmsg:
                started, completed, success = True, True, True
            if "Creating analysis plots" in firstmsg:
                started = True
                nresults = int(firstmsg.split("Creating analysis plots for results of length:")[-1])
                completed = len(table) > nresults
            error = any(["Traceback" in msg for msg in table["msg"]])
            success = not error and started and completed
            stepname = "analysis"

        return started, completed, success, stepname

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return f"Logs({self.name}, n={len(self.data)}, success={self.success})"

    @property
    def groups(self):
        """Table groups."""
        return self.data.groups

    @property
    def start(self):
        """Return the earliest timestep."""
        if len(self.data) > 0:
            return self.data[0]["time"]
        return None

    @property
    def end(self):
        """Return the last timestep."""
        if len(self.data) > 0:
            return self.data[-1]["time"]
        return None

    def sort(self, *args, **kwargs):
        """Sort the underlying table of logs.

        See help(Table.sort) for more.
        """
        self.data.sort(*args, **kwargs)

    def select(self, **kwargs):
        """Select from the logs where column matches the value.

        For example

        ```python
        Log.select(stepname="name", success=False)
        ```
        
        will select from the log entries all those whose name is
        ``name`` and success is `False`.

        Parameters
        ----------
        **kwargs :
            A list of keyword arguments and literal values on which the
            table will be subselected on.
        """
        mask = np.ones((len(self.data), ), dtype=bool)
        for arg, val in kwargs.items():
            mask = np.logical_and(mask, self.data[arg] == val)
        return self.data[mask]

    def group_by(self, *args, **kwargs):
        """Group table entries by predicate.

        See help(Table.group_by) for more.
        """
        self.data = self.data.group_by(*args, **kwargs)

    def get_artist(self, y=0, relative_to=None, units="hour",  # Data params
                   marker="o",  linestyle="--", name_pos="right", name_margin=None,
                   **kwargs):
        """Return the Log as a Timeline artist.

        Parameters
        -----------
        y : `int`, optional
        relative_to: `None`, optional
        units : `str`, optional
        marker : `str`, optional
        linestyle : `str`, optional
        name_pos : `str`, optional
        name_margin : `None`, optional
        **kwargs
        """
        if len(self.data) == 0:
            return None

        # Split up logs into groups based on execution location
        infologs = self.select(level="INFO")
        infologs.sort(["time", "stepname"])
        infologs = infologs.group_by(["stepname", "stepidx"])

        #grps = infologs.group_by(["process", "thread"])
        start_times = {i: g[0]["time"] for i, g in enumerate(infologs.groups)}
        ordered_grp_idxs = sorted(start_times, key=lambda k: start_times[k])

        # Parse each step into line kwargs for viz purposes
        # This is a mix of what we know and best guess attempts
        # a list of dictionaries, each of which is a
        # full specification of a line segment where
        # name is the value of the text printed above the
        # line itself
        linespec = [
        ]

        xvals, yvals = [], []
        for logidx, grpidx in enumerate(ordered_grp_idxs):
            grp = infologs.groups[grpidx]
            started, completed, success = self.started[logidx], self.completed[logidx], self.success[logidx]
            stepname = self.stepnames[logidx]
                
            xvals = [grp[0]["time"], grp[-1]["time"]]
            yvals = [y, y]

            if started and completed and success:
                segment_color = "green"
            elif started and completed and not success:
                segment_color = "red"
            if started and not completed:
                segment_color = "orange"

            linespec.append({
                "name": stepname,
                "xdata": xvals,
                "ydata": yvals,
                "color": segment_color,
                "linestyle": "-",
                "marker": marker
            })

        successfull_steps = self.stepnames[self.success]
        if not all([
                "resample" in successfull_steps,
                "search" in successfull_steps,
                "analysis" in successfull_steps
        ]):
            color = "red"
        else:
            color = "green"

        return Timeline(
            linespec,
            self.name,
            name_pos=name_pos,
            name_margin=name_margin,
            relative_to=relative_to,
            units=units,
            marker=marker,
            linestyle=linestyle,
            color=color,
            **kwargs
        )


def parse_logfile(logfile, fmt=Log.fmt):
    """Parse a single log file given a string format.

    The parsing is perfomed by regex using named groups.

    Parameters
    ----------
    logfile : `str`
        Path to the log file.
    fmt : `str`
        Regex matching string.

    Returns
    -------
    parsed : `dict`
        Dictionary containing parsed logs. Dictionary containing the
        named groups as keys. Each key contains an list of values
        extracted from each row of the log.
    """
    stmt = re.compile(fmt)
    logs = {k: [] for k in stmt.groupindex}
    with open(logfile) as of:
        for line in of.readlines():
            strmatch = re.search(stmt, line)
            if strmatch is not None:
                groups = strmatch.groupdict()
                for k in logs:
                    logs[k].append(groups[k])
            else:
                logs["msg"][-1] += line.strip()
    return logs


def parse_logdir(dirpath="."):
    """Parse every file ending in ``log`` from the given directory.

    This is not a generic function, logs are expected to be named as
    ``{collname}[.step].log``. Log is expected to be parsable by the
    `Log.log_fmt` regex string.

    Parameters
    ----------
    dirpath : `str`
        Path to the directory.

    Returns
    --------
    logs : `list[dict]`
        List of logs, each belonging to individual parsed log. Each
        parsed log is a dict. Keys of the dict are the names of the
        regex named groups. Values of the dict are lists containing the
        regex parsed value of the group for that line.
    """
    glob_stmt = os.path.join(dirpath, "*log")
    lognames = glob.glob(glob_stmt)

    collnames = {}
    for f in lognames:
        collname = os.path.basename(f)
        collname = collname.split(".")[0]
        if collname in collnames:
            collnames[collname].append(f)
        else:
            collnames[collname] = [f, ]

    logs = []
    for collname, logpaths in collnames.items():
        logs.append(Log(logpaths, name=collname))

    return sorted(logs, key=lambda l: l.start)


class Timeline(plt.Line2D):
    """A Gantt timeline artist.

    A Gantt timeline is a Matplotlib Line artist containing multiple
    lines. Line segments represent sub-task durations of a single
    Task, represented by the timeline.
    
    `Log`s represent themselves using the timeline matching to the
    earliest and latest timestamp found in the logs. Each step in a
    `Log` is a sub-length of the timeline with its own start and end
    points.

    A Timeline will be colored green if all the steps have started,
    completed and raised no errors.
    A Timeline segment will be colored yellow if a step has started,
    but not completed, and has not raised an Error. 
    A Timeline will be colored red if any of the steps have not
    completed.

    With each Timeline, a name is associated and plotted next to the
    full timeline as a Text artist.
    
    Parameters
    ----------
    linespec : `list[dict]`
        A list of dictionaries containing the data and the style of
        a Timeline segment. The expected keys are ``name``, ``xdata``,
        ``ydata``, but providing additional matplotlib accepted
        keywords like ``color``, ``linestyle`` and ``marker`` is
        allowed. The name is registered as the name of the Timeline
        segment. The ``xdata`` and ``ydata`` are expected to be `Time`
        objects.
    name : `str`
        Name of the Timeline.
    name_pos: `str`, optional.
        Position of the Timeline name, can be given as an `Time`,
        `datetime` or strings ``right`` or ``left``. By default
        all timeline names are plotted on the ``right`` of the timeline
        line, at the time of the latest timestamp in the linespec.
    name_margin : `float` or `None`, optional.
        Offset from the given position, in seconds. By default is `None`.
    name_fontsize : `float`, optional
        Fontsize of the Timeline name.
    relative_to : `Time`, `datetime` or `None`, optional
        Express all timestamps in terms of elapsed time relative to an
        origin. When `None` the full timestamp values are plotted instead
        of an elapsed time.
    units : `str`, optional
        When ``relative_to`` is given, controls the units in which the
        elapsed time is given in. Can be any string accepted by Astropy
        `units`. Default: ``hours``
    color : `str`, optional
        Timeline color, this will color the underlying line and the
        associated name text in the given color. Default: ``black``
    *args, **kwargs :
        Any additional, Matplotlib accepted, formatting options that
        will be applied to the timeline line.
    """
    def __init__(self, linespec,
                 name, name_pos="right", name_margin=None, name_fontsize=9,
                 relative_to=None, units="hour",
                 color="black", *args, **kwargs):
        # We must marshal everything into datetime because time_support doesn't
        # seem to be working. If we get a TimeDelta, marshal the values into
        # the same units, and then strip the units into floats. Work strictly
        # with datetime objects internally because those are the only ones
        # Matplotlib plots natively
        self.units = units
        self.origin = relative_to

        xvals, yvals = [], []
        for line in linespec:
            if self.origin is not None:
                line["xdata"] = [(i-self.origin).to_value(units) for i in line["xdata"]]
            else:
                line["xdata"] = [i.datetime for i in line["xdata"]]
            xvals.extend(line["xdata"])
            yvals.extend(line["ydata"])

        self.name_pos = name_pos
        if isinstance(name_pos, Time):
            self.name_pos = name_pos.datetime

        txtx, txty = self._get_name_pos(name_pos, name_margin, xvals, yvals, relative_to)
        self.text = plt.Text(txtx, txty, name, verticalalignment="center",
                             fontsize=name_fontsize, name_color=color)
        self.text.set_text(name)

        # Leverage the math support of Time objects by casting this at the end
        if isinstance(relative_to, Time):
            self.origin = relative_to.datetime

        self.linespec = linespec
        # The line segments can now be used blindly regardless of whether
        # we have a datetime objects, or floats
        self.name_margin = name_margin
        self.lines = []
        for line in linespec:
            name = line.pop("name")
            self.lines.append(plt.Line2D(**line))

        super().__init__(xvals, yvals, *args, color=color, **kwargs)

    def _get_name_pos(self, name_pos, name_margin, xvals, yvals, relative_to):
        """Resolve name position.

        If the given position is the literal ``left`` or ``right`` find
        the earliest and latest timestamp respectively, add the margin
        and return the coordinate of the text.

        If the given position is a `Time` or `Datetime` object, resolve
        relative offset to origin, add margin and return position.
        
        Parameters
        ----------
        name_pos: `str`
            Position of the Timeline name, can be given as an `Time`,
            `datetime` or strings ``right`` or ``left``. By default
            all timeline names are plotted on the ``right`` of the
            timeline line, at the time of the latest timestamp in the
            linespec.
        name_margin : `float` or `None`
            Offset from the given position, in seconds.
            By default is `None`.
        xvals : `list[Time]`
            List of all timestamps on the timeline.
        yvals : `list[int]`
            Y-index of the timeline on the plot.
        relative_to : `Time`, `datetime` or `None`, optional
            Express all timestamps in terms of elapsed time relative
            to an origin. When `None` the full timestamp values are
            plotted instead of an elapsed time.        

        Returns
        -------
        txtx : `datetime`
            X-coordinate in datetime of the name text.
        txty : `int`
            Y-index of the text.
        """
        txty = yvals[0]
        if name_pos == "right":
            txtx = xvals[-1]
        elif name_pos == "left":
            txtx = xvals[0]
        else:
            txtx = name_pos
            self.txtx = name_pos

        if relative_to is not None and not isinstance(txtx, float):
            txtx = TimeDelta(txtx - relative_to)

        if isinstance(txtx, TimeDelta):
            txtx = txtx.to_value(self.units)

        if isinstance(txtx, Time):
            txtx = txtx.datetime

        if name_margin is not None:
            margin = TimeDelta(name_margin, format="sec")
            if isinstance(txtx, float):
                txtx = txtx + margin.to_value(self.units)
            else:
                txtx = txtx + margin.datetime

        return txtx, txty

    ## Matplotlib API requirements. Set figure, axes, transform, data,
    #  make values outside of the axes invisible and draw.
    def set_figure(self, figure):
        self.text.set_figure(figure)
        [line.set_figure(figure) for line in self.lines]
        super().set_figure(figure)

    @plt.Line2D.axes.setter
    def axes(self, new_axes):
        self.text.axes = new_axes
        self.text.set_clip_box(new_axes.bbox)
        #self.text.set_clip_on(True)
        # Call the superclass property setter.
        for line in self.lines:
            plt.Line2D.axes.fset(line, new_axes)
            line.set_clip_box(new_axes.bbox)
            line.set_clip_on(True)
        plt.Line2D.axes.fset(self, new_axes)

    def set_transform(self, transform):
        self.text.set_transform(transform)
        [l.set_transform(transform) for l in self.lines]
        super().set_transform(transform)

    def set_data(self, x, y):
        super().set_data((x[0], x[-1]), (y[0], y[-1]))

    def draw(self, renderer):
        super().draw(renderer)
        self.text.draw(renderer)
        [line.draw(renderer) for line in self.lines]

        
def plot_campaign(ax, logs, relative_to_launch=True, units="hour",
         name_pos="right", name_margin=4,
         **kwargs):
    """Plot a campaign, a collection of `Logs`, onthe given axis.

    Parameters
    ----------
    ax : `Axes`
        Matplotlib axis to contain the plot.
    logs : `list[Log]`
        Collection of logs which timelines will be plotted.
    relative_to_launch : `bool`, optional
        When `True` all of the timelines are expressed in ``units``
        elapsed starting from the earliest timestamp.
    units : `str`
        Units in which the elapsed time is expressed in, by default
        ``hours``. Can be any Astropy `units` accepted string.
    name_pos : `str` or `Time`, optional
        Position of the Timeline name. See `help(Timeline)` for more.
    name_margin : `float` or None
        Additional name margin. See `help(Timeline)` for more.
    **kwargs :
        Any additional kwargs are passed onto the `Log.get_artist` call.

    Returns
    -------
    ax : `Axes`
        Matplotlib axis containing the plot.
    """
    # starts are sorted, but ends are not neccessarily in order
    workflow_start = logs[0].start
    workflow_end = max([log.end for log in logs])
    duration = (workflow_end - workflow_start).to_value(units)
    ax.set_title(f"Launched {workflow_start}; Finished {workflow_end}\n Duration {duration:2.4}{units}")

    relative_to = None
    if relative_to_launch:
        relative_to = workflow_start

    align_style = None
    align_pos = name_pos
    if "+" in name_pos:
        align_pos, align_style = name_pos.split("+")

    if align_style == "column":
        if align_pos == "right":
            align_pos = workflow_end
        if align_pos == "left":
            align_pos = workflow_start #- TimeDelta(3600, format="sec")

    for i, log in enumerate(logs):
        timeline = log.get_artist(
            y=i,
            relative_to=relative_to,
            name_pos=align_pos,
            name_margin=name_margin,
            units=units,
            **kwargs
        )
        ax.add_artist(timeline)
        
    if relative_to_launch:
        ax.set_xlim(-0.2, duration+duration/10)
    else:
        margin = TimeDelta(3800, format="sec")
        ax.set_xlim((workflow_start-margin).datetime, (workflow_end+margin).datetime)

    ax.set_ylim(-1, i+1)
    ax.set_xlabel(f"Time ({units})")
    ax.set_ylabel("Tasks")
    
    return ax
