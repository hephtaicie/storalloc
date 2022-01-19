""" Storalloc
    Default logging utilities
"""
import sys
import logging

from logging import StreamHandler, Formatter
from logging.handlers import RotatingFileHandler

import zmq
from zmq.log.handlers import PUBHandler

LOGGER_NAME = "storalloc"


def get_storalloc_logger(verbose: bool = False, stderr_log: bool = False, logger_name: str = ""):
    """Return a storalloc logger with proper configuration for local logging"""

    if not logger_name:
        logger = logging.getLogger(LOGGER_NAME)
    else:
        logger = logging.getLogger(logger_name)

    if logger.hasHandlers():
        # We can assume it has already been configured
        return logger

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Configure handlers and formatters for logger
    formatter = Formatter("%(asctime)s - [%(levelname)s][%(module)s:%(lineno)d] :: %(message)s")

    if stderr_log:
        # Log to stderr
        stream_hdl = StreamHandler(stream=sys.stderr)
        stream_hdl.setFormatter(formatter)
        logger.addHandler(stream_hdl)

    # Log to file
    rotating_file_hdl = RotatingFileHandler(
        "storalloc.log", maxBytes=5000000, encoding="utf-8", backupCount=4
    )
    rotating_file_hdl.setFormatter(formatter)

    logger.addHandler(rotating_file_hdl)

    return logger


def add_remote_handler(
    logger: logging.Logger, topic: str, context: zmq.Context, url: str, sync_url: str
):
    """Configure an existing logger with additional remote handler using PyZMQ"""

    # Prepare handler
    log_publisher = context.socket(zmq.PUB)  # pylint: disable=no-member
    log_publisher.connect(url)
    handler = PUBHandler(log_publisher)
    handler.root_topic = topic.encode("utf-8")

    # Synchronisation -> we expect the log-server to be already up and running
    sync_socket = context.socket(zmq.DEALER)  # pylint: disable=no-member
    sync_socket.connect(sync_url)
    sync_socket.send_multipart([topic.encode("utf-8")])
    res = sync_socket.poll(timeout=1000)
    if not res:
        logger.warning("Unable tor each log-server, remote logging deactivated")
        return False

    logger.addHandler(handler)  # pylint: disable=no-member
    logger.info("Remote logging configured")
    return True
