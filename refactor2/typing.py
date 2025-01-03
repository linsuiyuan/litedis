from enum import Enum
from typing import Protocol, NamedTuple

LitedisObjectT = dict | list | set | str


class PersistenceType(Enum):
    NONE = "none"
    AOF = "aof"
    LDB = "ldb"
    MIXED = "mixed"

class DBCommandLine(NamedTuple):
    dbname: str
    cmdline: str

class CommandLogger(Protocol):
    def log_command(self, dbcmd: DBCommandLine): ...


class CommandProcessor(Protocol):
    def process_command(self, dbcmd: DBCommandLine): ...
