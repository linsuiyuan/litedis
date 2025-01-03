from enum import Enum
from typing import Protocol

LitedisObjectT = dict | int | list | set | str


class PersistenceType(Enum):
    NONE = "none"
    AOF = "aof"
    LDB = "ldb"
    MIXED = "mixed"


class CommandLogger(Protocol):
    def log_command(self, dbname: str, cmdline: str): ...


class CommandProcessor(Protocol):
    def process_command(self, dbname: str, cmdline: str): ...

    def replay_command(self, dbname: str, cmdline: str): ...
