from enum import Enum
from typing import Protocol, NamedTuple

from refactor2.core.command.sortedset import SortedSet

LitedisObjectT = dict | list | set | str | SortedSet

DB_COMMAND_SEPARATOR = "="


class ReadWriteType(Enum):
    Read = "read"
    Write = "write"


class DBCommandPair(NamedTuple):
    dbname: str
    cmdtokens: list[str]


class CommandProcessor(Protocol):
    def process_command(self, dbcmd: DBCommandPair): ...
