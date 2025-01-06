from enum import Enum
from typing import Protocol, NamedTuple

LitedisObjectT = dict | list | set | str


class ReadWriteType(Enum):
    Read = "read"
    Write = "write"


class DBCommandTokens(NamedTuple):
    dbname: str
    cmdtokens: str


class CommandProcessor(Protocol):
    def process_command(self, dbcmd: DBCommandTokens): ...
