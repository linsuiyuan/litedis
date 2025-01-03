from pathlib import Path
from typing import Any

from refactor2.client.commands import BasicKeyCommand
from refactor2.commandline import combine_command_line
from refactor2.server.dbmanager import DBManager
from refactor2.typing import CommandProcessor, DBCommandLine


class Litedis(BasicKeyCommand):

    def __init__(self,
                 dbname: str = "db",
                 data_path: str | Path = "ldbdata"):
        self.dbname = dbname
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)

        dbmanager = DBManager(self.data_path)

        self.executor: CommandProcessor = dbmanager

    def execute_command(self, *args) -> Any:
        command_line = combine_command_line(args)
        result = self.executor.process_command(DBCommandLine(self.dbname, command_line))
        return result
