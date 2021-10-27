""" Storalloc
    Default message implementation

    Pickle is well known for its security vulnerabilities,
    we may want to move to another serialisation format at
    some point (protobuf ?)
"""

import pickle


class Message:
    """Default message implementation"""

    def __init__(self, msg_type, msg_content):

        self.type = msg_type
        self.content = msg_content

    def __str__(self):
        """String representation for debugging purpose"""
        return f"{self.type}-{self.content}"

    @staticmethod
    def from_packed_message(packed_data):
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
