#!/usr/bin/env python3

import argparse
import logging
import sys
import os
import yaml
import zmq
import re
import datetime
from shutil import which

from src.message import Message
from src.config_file import ConfigFile

# Default values
req_size = 0
req_time = 0
conf_file = None
start_time = None
eos = False


def cli_error(parser, message):
    parser.print_usage()
    print(message)
    sys.exit(1)


def parse_args():
    """Parse arguments given as input on the command line"""
    global req_size, req_time, conf_file, start_time, eos

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path of the StorAlloc configuration file (YAML)")
    parser.add_argument(
        "-s", "--size", type=int, help="Size of the requested storage allocation (GB)"
    )
    parser.add_argument("-t", "--time", help="Total run time of the storage allocation (HH:MM:SS)")
    parser.add_argument(
        "--start_time", help="Timestamp of the allocation's starting time (Simulation only)"
    )
    parser.add_argument(
        "--eos",
        help="Send EndOfSimulation flag to the orchestrator (Simulation only)",
        action="store_true",
    )
    parser.add_argument("-v", "--verbose", help="Display debug information", action="store_true")

    args = parser.parse_args()

    if not args.config:
        cli_error(parser, "Error: argument --config (-c) is mandatory!")
    else:
        conf_file = ConfigFile(args.config)

    if not args.size or args.size == 0:
        cli_error(parser, "Error: argument --size (-s) is mandatory!")
    else:
        req_size = args.size

    if not args.time:
        cli_error(parser, "Error: argument --time (-t) is mandatory!")
    else:
        try:
            duration = datetime.datetime.strptime(args.time, "%H:%M:%S")
            req_time = int(
                datetime.timedelta(
                    hours=duration.hour, minutes=duration.minute, seconds=duration.second
                ).total_seconds()
            )
        except:
            cli_error(parser, "Error: Time format is wrong!")

    if args.start_time:
        if re.match("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", args.start_time):
            start_time = args.start_time
        else:
            cli_error(parser, "Error: Allocation's starting time format is wrong!")

    if args.eos:
        eos = True

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="[D] %(message)s")


def build_request(size, time, start_time=None):
    """Concatenate request details in a comma-separated format"""
    return str(size) + "," + str(time) + "," + str(start_time)


def orchestrator_url():
    return "tcp://" + conf_file.get_orch_ipv4() + ":" + str(conf_file.get_orch_port())


def main(argv):
    """Main loop

    Send a request according to the arguments given on the command line
    to the orchestrator and receive messages from this latter until a
    shutdown/error message is received or resources have been allocated.
    """

    parse_args()

    context = zmq.Context()
    sock = context.socket(zmq.DEALER)
    sock.connect(orchestrator_url())

    request = build_request(req_size, req_time, start_time)

    if eos:
        message = Message("eos", request)
    else:
        message = Message("request", request)

    logging.debug("Submitting request [" + request + "]")
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


if __name__ == "__main__":
    main(sys.argv[1:])
