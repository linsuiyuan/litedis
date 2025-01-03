from typing import Protocol, NamedTuple

LitedisObjectT = dict | list | set | str


class DBCommandLine(NamedTuple):
    dbname: str
    cmdline: str


class CommandProcessor(Protocol):
    def process_command(self, dbcmd: DBCommandLine): ...
