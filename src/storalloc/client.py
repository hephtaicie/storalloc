""" Storalloc
    Default Client
"""

import datetime
import logging
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
    log.info(f"Request : {request}")

    if eos:
        message = Message("eos", request)
    else:
        message = Message("request", request)

    logging.debug(f"Submitting request [{request}]")

    sock.send(message.pack())

    while True:
        data = sock.recv()
        message = Message.from_packed_message(data)

        if message.type == "notification":
            print(f"storalloc: {message.content}")
        elif message.type == "allocation":
            print(f"storalloc: {message.content}")
            # Do stuff with connection details
            break
        elif message.type == "error":
            print(f"storalloc: [ERR] {message.content}")
            break
        elif message.type == "shutdown":
            print("storalloc: closing the connection at the orchestrator's initiative")
            break

    sock.close(linger=0)
    context.term()
