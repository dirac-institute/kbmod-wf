import time
import traceback
import logging
from logging import config


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
        "parsl": {"level": "INFO"},
        "task": {"level": "DEBUG", "handlers": ["stdout"], "propagate": False},
        "task.create_manifest": {},
        "task.ic_to_wu": {},
        "task.reproject_wu": {},
        "task.kbmod_search": {},
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
    """Logs received errors before re-raising them.

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




import os
import re
import glob

import numpy as np
import matplotlib.pyplot as plt
from astropy.table import Table, vstack
from astropy.time import Time, TimeDelta


class Timeline(plt.Line2D):
    def __init__(self, linespec, name, name_pos="right", name_margin=None, name_fontsize=9,
                 relative_to=None, units="hour", color="black", *args, **kwargs):
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
        self.text = plt.Text(txtx, txty, name, verticalalignment="center", fontsize=name_fontsize, color=color)
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

    def set_figure(self, figure):
        self.text.set_figure(figure)
        [line.set_figure(figure) for line in self.lines]
        super().set_figure(figure)

    # Override the Axes property setter to set Axes on our children as well.
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


def parse_logfile(logfile):
    stmt = re.compile(Log.fmt)
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


class Log:
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
        return self.data.groups

    @property
    def start(self):
        if len(self.data) > 0:
            return self.data[0]["time"]
        return None

    @property
    def end(self):
        if len(self.data) > 0:
            return self.data[-1]["time"]
        return None

    def sort(self, *args, **kwargs):
        self.data.sort(*args, **kwargs)

    def select(self, **kwargs):
        mask = np.ones((len(self.data), ), dtype=bool)
        for arg, val in kwargs.items():
            mask = np.logical_and(mask, self.data[arg] == val)
        return self.data[mask]

    def group_by(self, *args, **kwargs):
        self.data = self.data.group_by(*args, **kwargs)

    def get_artist(self, y=0, relative_to=None, units="hour",  # Data params
                   marker="o",  linestyle="--", name_pos="right", name_margin=None,
                   **kwargs):

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


def parse_logdir(dirpath="."):
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


def plot_campaign(ax, logs, relative_to_launch=True, units="hour",
         name_pos="right", name_margin=4,
         **kwargs):
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
