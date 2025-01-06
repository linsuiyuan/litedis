from enum import Enum
from typing import Protocol, NamedTuple

LitedisObjectT = dict | list | set | str

DB_COMMAND_SEPARATOR = "="


class ReadWriteType(Enum):
    Read = "read"
    Write = "write"


class DBCommandTokens(NamedTuple):
    dbname: str
    cmdtokens: list[str]


class CommandProcessor(Protocol):
    def process_command(self, dbcmd: DBCommandTokens): ...
