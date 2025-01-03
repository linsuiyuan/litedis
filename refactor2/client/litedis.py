from pathlib import Path
from typing import Any

from refactor2.client.commands import BasicKeyCommand
from refactor2.server.dbmanager import DBManager
from refactor2.typing import PersistenceType, CommandProcessor


class Litedis(BasicKeyCommand):

    def __init__(self,
                 dbname: str = "db",
                 data_path: str | Path = "ldbdata",
                 persistence=PersistenceType.MIXED):
        self.dbname = dbname
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)

        dbmanager = DBManager(self.data_path,
                              persistence_type=persistence)

        self.executor: CommandProcessor = dbmanager

    def execute_command(self, *args) -> Any:
        command_line = self._combine_command_args(*args)
        result = self.executor.process_command(self.dbname, command_line)
        return result

    def _combine_command_args(self, *args):
        args = self._strip_and_quote_command_args(*args)
        return " ".join(args)

    def _strip_and_quote_command_args(self, *args):
        result = []
        for arg in args:
            arg = arg.strip()
            if " " in arg:
                arg = f'"{arg}"'
            result.append(arg)
        return result
