from pathlib import Path
from typing import Any, Union

from litedis.client.commands import (
    BasicCommands,
    HashCommands,
    ListCommands,
    SetCommands,
    ZSetCommands
)
from litedis.core.dbmanager import DBManager
from litedis.typing import CommandProcessor, DBCommandPair


class Litedis(
    BasicCommands,
    HashCommands,
    ListCommands,
    SetCommands,
    ZSetCommands
):
    def __init__(self,
                 dbname: str = "db",
                 persistence_on: bool = True,
                 data_path: Union[str, Path] = "ldbdata"):
        self.dbname = dbname

        dbmanager = DBManager(data_path, persistence_on=persistence_on)

        self.executor: CommandProcessor = dbmanager

    def execute(self, *args) -> Any:
        result = self.executor.process_command(DBCommandPair(self.dbname, list(args)))
        return result
