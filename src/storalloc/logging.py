""" Storalloc
    Default logging utilities
"""
import sys
import logging
from logging import StreamHandler, Formatter
from logging.handlers import RotatingFileHandler

LOGGER_NAME = "storalloc"


def get_storalloc_logger(verbose: bool = True):
    """Return a storalloc logger with proper configuration"""

    logger = logging.getLogger(LOGGER_NAME)
    if logger.hasHandlers():
        # We can assume it has already been configured
        return logger

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Configure handlers and formatters for logger
    formatter = Formatter("%(asctime)s - [%(levelname)s][%(module)s:%(lineno)d] :: %(message)s")

    stream_hdl = StreamHandler(stream=sys.stderr)
    stream_hdl.setFormatter(formatter)

    rotating_file_hdl = RotatingFileHandler(
        "storalloc.log", maxBytes=1000000, encoding="utf-8", backupCount=4
    )

    logger.addHandler(stream_hdl)
    logger.addHandler(rotating_file_hdl)

    return logger
