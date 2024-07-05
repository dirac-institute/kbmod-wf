DEFAULT_FORMAT = (
    "%(created)f %(asctime)s %(processName)s-%(process)d "
    "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s %(levelname)s: "
    "%(message)s"
)


def configure_logger(name, file_path):
    """
    Simple function that will create a logger object and configure it to write
    to a file at the specified path.
    Note: We import logging within the function because we expect this to be
    called within a parsl app."""

    import logging

    logger = logging.getLogger(name)
    handler = logging.FileHandler(file_path)
    formatter = logging.Formatter(DEFAULT_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger
