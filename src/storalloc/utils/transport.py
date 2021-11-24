""" Transport facade for PyZMQ
"""

import zmq

from storalloc.utils.message import Message


class Transport:
    """Transport abstraction over PyZMQ using our custom Messages with binary serialisation"""

    def __init__(self, socket: zmq.Socket):
        self.socket = socket

    def send_multipart(self, message: Message, prefix: str = None):
        """Send multipart message with emulated enveloppe"""
        if prefix and isinstance(prefix, str):
            # If a destination is specified, it will be added as the first frame of the message
            self.socket.send_multipart([prefix.encode("utf-8"), message.pack()])
        if prefix and isinstance(prefix, list):
            # If a prefix is a list, it wil be appended as suche before the message
            self.socket.send_multipart([dest.encode("utf-8") for dest in prefix] + [message.pack()])
        else:
            # If no logical destination is specified, we suppose the message is sent through a
            # DEALER
            self.socket.send_multipart([message.pack()])

    def send_sync(self, destination: bytes = None):
        """Send empty sync message"""
        if destination:
            self.socket.send_multipart([destination, b""])
        else:
            self.socket.send_multipart([b""])

    def recv_multipart(self):
        """Receive multipart message"""
        frames = self.socket.recv_multipart()
        message = frames.pop()  # Last frame is always the content, first frame(s) is/are the md
        identities = frames  # identities/prefix, if any, are what's left of the frame list
        return ([ident.decode("utf-8") for ident in identities], Message.unpack(message))

    def recv_sync_router(self):
        """Receive sync message on ROUTER"""
        sync_msg = self.socket.recv_multipart()
        return sync_msg[0]

    def poll(self, timeout: int = 0):
        """Poll socket (just so that we can call
        Transport.poll instead of Transport.socket.poll..."""
        return self.socket.poll(timeout)
