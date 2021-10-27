""" Storalloc
    Default message implementation
"""

import pickle


class Message:
    """Default message implementation"""

    def __init__(self, msg_type, msg_content):

        self.type = msg_type
        self.content = msg_content

    @classmethod
    def from_packed_message(cls, packed_data):
        """Extract message from pickled data"""
        return pickle.loads(packed_data)

    def pack(self):
        """Pack message into pickle"""
        return pickle.dumps(self)

    def send(self, socket, identities):
        """Send message..."""
        if isinstance(identities, list):
            socket.send_multipart(identities + [self.pack()])
        else:
            socket.send_multipart([identities, self.pack()])
