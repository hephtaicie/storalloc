"""Log server for collecting logs from pyzmq PUBHandler in other components"""

from random import randint

import zmq
from sty import fg

from storalloc.utils.config import config_from_yaml
from storalloc.utils.logging import LOGGER_NAME


# pylint: disable=no-member


class LogServer:
    """Small logging utility for aggregating logs from various components"""

    def __init__(self, config_path: str, verbose: bool = True):
        """Init a log server"""

        self.conf = config_from_yaml(config_path)
        self.verbose = verbose

        self.context = zmq.Context()

        # Logging SUBSCRIBER
        self.log_sub = self.context.socket(zmq.SUB)
        self.log_sub.setsockopt(zmq.SUBSCRIBE, LOGGER_NAME.encode("utf-8"))
        self.log_sub.bind(f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_port']}")

        # Synchronisation ROUTER
        self.sync_signal = self.context.socket(zmq.ROUTER)
        self.sync_signal.bind(
            f"tcp://{self.conf['log_server_addr']}:{self.conf['log_server_sync_port']}"
        )

        self.poller = zmq.Poller()
        self.poller.register(self.log_sub, zmq.POLLIN)
        self.poller.register(self.sync_signal, zmq.POLLIN)

        self.colors = {}

    def run(self):
        """Start listening for logs"""

        while True:
            try:

                events = dict(self.poller.poll())
                if events.get(self.log_sub) == zmq.POLLIN:
                    log = self.log_sub.recv_multipart()
                    topic_severity, message = log[0], log[1]
                    topic, severity = topic_severity.decode("utf-8").split(".")
                    print(
                        f"{self.colors[topic]}"
                        + f"{topic}: [{severity}] {message.decode('utf-8')}{fg.rs}",
                        end="",
                    )

                if events.get(self.sync_signal) == zmq.POLLIN:
                    sync_msg = self.sync_signal.recv_multipart()
                    identity, topic = sync_msg[0], sync_msg[1]
                    self.log_sub.setsockopt(zmq.SUBSCRIBE, topic)
                    dtopic = topic.decode("utf-8")
                    if dtopic not in self.colors:
                        self.colors[dtopic] = fg(
                            randint(70, 255), randint(70, 255), randint(70, 255)
                        )
                    self.sync_signal.send_multipart([identity, b""])

            except zmq.ZMQError as err:
                if err.errno == zmq.ETERM:
                    break
                raise
            except KeyboardInterrupt:
                print("[!] Log Server terminated by Ctrl-C")
                break

        self.context.destroy()
