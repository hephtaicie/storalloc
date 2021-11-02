""" Storalloc
    Default message implementation.
    Basically a wrapper for binary serialisation.
    Allows classification of content by category
"""

from dataclasses import dataclass
from enum import Enum

import msgpack


class MsgCat(Enum):
    """Allowed message category"""

    NOTIFICATION = 1
    ERROR = 2
    REQUEST = 3
    ALLOCATION = 4
    REGISTRATION = 5
    CONNECTION = 6
    DEALLOCATION = 7
    EOS = 10
    SHUTDOWN = 16


@dataclass
class Message:
    """Default message implementation"""

    category: MsgCat
    content: "typing.Any"

    def __str__(self):
        """String representation for debugging purpose"""
        return f"{self.category.name}-{self.content}"

    @classmethod
    def unpack(cls, packed_data: bytes):
        """Extract message from packed data"""
        category_id, content = msgpack.unpackb(packed_data, use_list=False)
        return cls(MsgCat(category_id), content)

    def pack(self):
        """Pack message into MessagePack serialised data"""
        return msgpack.packb([self.category.value, self.content])
