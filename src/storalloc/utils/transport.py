""" Transport facade for PyZMQ
"""

import zmq

from storalloc.utils.message import Message


class Transport:
    """Transport abstraction over PyZMQ using our custom Messages with binary serialisation"""

    def __init__(self, socket: zmq.Socket, source_id: str):
        self.socket = socket
        self.uid = source_id

    def send_multipart(self, message: Message):
        """Send multipart message with emulated enveloppe"""
        self.socket.send_multipart([self.uid, b"", message.pack()])

    def recv_multipart(self):
        """Receive multipart message"""
        frames = self.socket.recv_multipart()
        message = frames.pop()
        frames.pop()  # remove empty frame
        identities = frames
        return (identities, Message.unpack(message))
