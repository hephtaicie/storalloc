""" Transport facade for PyZMQ
"""

import zmq

from storalloc.utils.message import Message


class Transport:
    """Transport abstraction over PyZMQ using our custom Messages with binary serialisation"""

    def __init__(self, socket: zmq.Socket):
        self.socket = socket

    def send_multipart(self, message: Message, destination_id: str = None):
        """Send multipart message with emulated enveloppe"""
        if destination_id and isinstance(destination_id, str):
            # If a destination is specified, it will be added as the first frame of the message
            self.socket.send_multipart([destination_id.encode("utf-8"), message.pack()])
        if destination_id and isinstance(destination_id, list):
            # If a destination_id is a list, it wil be appended as suche before the message
            self.socket.send_multipart(
                [dest.encode("utf-8") for dest in destination_id] + [message.pack()]
            )
        else:
            # If no logical destination is specified, we suppose the message is sent through a
            # DEALER
            self.socket.send_multipart([message.pack()])

    def recv_multipart(self):
        """Receive multipart message"""
        frames = self.socket.recv_multipart()
        print(f"Receive multi frames :  {frames}")
        message = frames.pop()
        # frames.pop()  # remove empty frame (always present
        identities = frames  # identities, if any are what's left of the frame list
        return ([ident.decode("utf-8") for ident in identities], Message.unpack(message))

    def poll(self, timeout: int = 0):
        """Poll socket"""
        return self.socket.poll(timeout)
