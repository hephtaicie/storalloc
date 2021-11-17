""" Transport facade for PyZMQ
    Used inside orchestrator
"""

from abc import ABC, abstractmethod

import zmq

from storalloc.message import Message


class AbstractTransport(ABC):
    """Abstract transport class"""

    def __init__(self, transport):
        self.transport = transport

    @abstractmethod
    def send_multipart(self, destination, content):
        pass

    @abstractmethod
    def recv_multipart(self):
        pass


#class Transport(AbstractTransport):
#    """Transport abstraction over PyZMQ using our custom Messages with binary serialisation"""
#
#    def __init__(self, socket):
#        self.socket = socket
#
#    def send_multipart(self, destination: str, content: Message):
#        """Send multipart message to destination"""
#        self.socket.send_multipart([destination, content.pack()])
#
#    def recv_multipart(self):
#        """Receive multipart message"""
#        parts = self.socket.recv_multipart()
#        identities, data = parts[:-1], parts[-1]
#        return (identities, Message.unpack(data))

