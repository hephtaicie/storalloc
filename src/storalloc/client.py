""" Storalloc
    Default Client
"""

import datetime
import logging
import yaml
import zmq

from storalloc.message import Message
from storalloc.config_file import ConfigFile


def zmq_init(conf: ConfigFile):
    """Connect to orchestrator with ZeroMQ"""

    context = zmq.Context()
    sock = context.socket(zmq.DEALER)
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

    conf = ConfigFile(config)

    context, sock = zmq_init(conf)

    request = f"{size},{time},{start_time}"

    if eos:
        message = Message("eos", request)
    else:
        message = Message("request", request)

    logging.debug(f"Submitting request [{request}]")

    sock.send(message.pack())

    while True:
        data = sock.recv()
        message = Message.from_packed_message(data)

        if message.get_type() == "notification":
            print("storalloc: " + message.get_content())
        elif message.get_type() == "allocation":
            print("storalloc: " + str(message.get_content()))
            # Do stuff with connection details
            break
        elif message.get_type() == "error":
            print("storalloc: [ERR] " + message.get_content())
            break
        elif message.get_type() == "shutdown":
            print("storalloc: closing the connection at the orchestrator's initiative")
            break

    sock.close(linger=0)
    context.term()
