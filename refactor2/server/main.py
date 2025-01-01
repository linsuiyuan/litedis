from pathlib import Path

from refactor2.server.commands import CommandContext
from refactor2.server.commands.parsers import parse_command_line_to_object
from refactor2.server.interfaces import CommandLogger
from refactor2.server.persistence import AOF
from refactor2.server.persistence import DBManager
from refactor2.typing import PersistenceType


class LitedisServer:

    def __init__(self,
                 data_path: str | Path,
                 persistence_type=PersistenceType.MIXED):

        self.persistence_type = persistence_type

        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)

        self.data_path.mkdir(parents=True, exist_ok=True)

        self.dbmanager = DBManager()
        self.command_logger: CommandLogger = self.dbmanager

    def process_command_line(self, dbname: str, cmdline: str):
        result = self._execute_command_line(dbname, cmdline)

        if self.command_logger:
            self.command_logger.log_command(dbname, cmdline)

        return result

    def _execute_command_line(self, dbname: str, cmdline: str):
        db = self.dbmanager.get_or_create_db(dbname)
        ctx = CommandContext(db)
        command = parse_command_line_to_object(cmdline)
        result = command.execute(ctx)
        return result

    def replay_command(self, dbname: str, cmdline: str):
        self._execute_command_line(dbname, cmdline)
