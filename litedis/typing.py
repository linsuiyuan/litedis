from enum import Enum
from typing import Protocol, NamedTuple, Union, List

from litedis.core.command.sortedset import SortedSet

LitedisObjectT = Union[dict, list, set, str, SortedSet]


class ReadWriteType(Enum):
    Read = "read"
    Write = "write"


class DBCommandPair(NamedTuple):
    dbname: str
    cmdtokens: List[str]


class CommandProcessor(Protocol):
    def process_command(self, dbcmd: DBCommandPair): ...
