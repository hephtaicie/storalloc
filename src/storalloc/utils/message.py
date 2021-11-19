""" Storalloc default message implementation.
    Basically a wrapper for binary serialisation using msgpack.

    Messages are composed of two parts:

    - category: give the receiver a hint on the purpose of the message and the content type.
    - content: can be anything as long as it is serialisable with msgpack.
      Type depend on the category.

    Some message categories are reserved for informational messages
    (which may lead to an action or not):
    - NOTIFICATION
    - ERROR
    - EOS
    - SHUTDOWN

    Others are reserved for transimitting actionnable information:
    - REQUEST ( client to orchestrator to server )  -> Information on the storage
      requested by / granted to client
    - REGISTRATION ( server to orchestrator ) -> Information on storage available on server

    Note : so far we don't add anything from ZMQ to the message class, but we might do it in
    future devs

"""

from dataclasses import dataclass, field
from enum import Enum

import msgpack


class MsgCat(Enum):
    """Allowed message category"""

    NOTIFICATION = 1
    ERROR = 2
    REQUEST = 3
    REGISTRATION = 4
    EOS = 10
    SHUTDOWN = 16


@dataclass
class Message:
    """Default message implementation
    A message has:
    - a category
    - a content (whose type is related to the message category, and must be serialisable)
    """

    category: MsgCat
    content: "typing.Any" = field(default="")  # any type AS LONG AS IT'S RECOGNISED BY MSGPACK

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

    @classmethod
    def notification(cls, content: str, pack: bool = False):
        """Craft a notification message ready to be sent"""
        if pack:
            return cls(MsgCat.NOTIFICATION, content).pack()
        return cls(MsgCat.NOTIFICATION, content)

    @classmethod
    def error(cls, content: str, pack: bool = False):
        """Craft an error message ready to be sent"""
        if pack:
            return cls(MsgCat.ERROR, content).pack()
        return cls(MsgCat.ERROR, content)
