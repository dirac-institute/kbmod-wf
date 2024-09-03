import traceback
import logging
from logging import config

__all__ = ["LOGGING_CONFIG", "get_configured_logger", "ErrorLogger"]


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
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "stderr": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "file": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.FileHandler",
            "filename": "parsl.log",
        },
    },
    "loggers": {
        "task": {"level": "INFO", "handlers": ["file", "stdout"], "propagate": False},
        "task.create_manifest": {},
        "task.ic_to_wu": {},
        "task.reproject_wu": {},
        "task.kbmod_search": {},
        "kbmod": {"level": "INFO", "handlers": ["file", "stdout"], "propagate": False},
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
