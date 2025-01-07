from pathlib import Path
from typing import Any

from refactor2.client.commands import BasicCommands, HashCommands, ListCommands, SetCommands, ZSetCommands
from refactor2.core.dbmanager import DBManager
from refactor2.typing import CommandProcessor, DBCommandTokens


class Litedis(
    BasicCommands,
    HashCommands,
    ListCommands,
    SetCommands,
    ZSetCommands
):
    def __init__(self,
                 dbname: str = "db",
                 data_path: str | Path = "ldbdata"):
        self.dbname = dbname
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)

        dbmanager = DBManager(self.data_path)

        self.executor: CommandProcessor = dbmanager

    def execute(self, *args) -> Any:
        result = self.executor.process_command(DBCommandTokens(self.dbname, list(args)))
        return result
