""" Storalloc
    Default Client
"""

import datetime
import zmq

from storalloc.message import Message
from storalloc.config_file import ConfigFile
from storalloc.logging import get_storalloc_logger


def zmq_init(conf: ConfigFile):
    """Connect to orchestrator with ZeroMQ"""

    context = zmq.Context()
    sock = context.socket(zmq.DEALER)  # pylint: disable=no-member
    sock.connect(f"tcp://{conf.get_orch_ipv4()}:{conf.get_orch_port()}")
    return (context, sock)


def run(
    config: str,
    size: int,
    time: datetime.datetime,
    start_time: datetime.datetime = None,
    eos: bool = False,
):
    """Init and start a new client"""

    log = get_storalloc_logger()

    conf = ConfigFile(config)

    context, sock = zmq_init(conf)

    request = f"{size},{time},{start_time}"
    log.info(f"New user request [{request}]")

    if eos:
        message = Message("eos", request)
    else:
        message = Message("request", request)

    log.debug(f"Submitting user message [{message}]")

    sock.send(message.pack())

    while True:
        data = sock.recv()
        message = Message.from_packed_message(data)

        if message.type == "notification":
            log.info(f"New notification received [{message.content}]")
        elif message.type == "allocation":
            log.info(f"New allocation received [{message.content}]")
            # Do stuff with connection details
            break
        elif message.type == "error":
            break
        elif message.type == "shutdown":
            log.warning("Orchestrator has asked to close the connection")
            break

    # should be part of some context manager ?
    sock.close(linger=0)
    context.term()
