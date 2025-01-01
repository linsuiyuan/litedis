from pathlib import Path

from refactor2.server.commands import CommandContext
from refactor2.server.commands.parsers import parse_command_line_to_object
from refactor2.server.persistence import AOF
from refactor2.server.persistence import DBManager
from refactor2.typing import PersistenceType


class LitedisServer:

    def __init__(self,
                 data_path: str | Path,
                 persistence_type=PersistenceType.MIXED):
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)

        self.persistence_type = persistence_type

        self.data_path.mkdir(parents=True, exist_ok=True)

        self.dbmanager = DBManager()
        self.aof: AOF | None = None
        self._init_aof()


    def _init_aof(self):
        if not self._need_aof_persistence():
            return
        self.aof = AOF(self.data_path)

    def _need_aof_persistence(self) -> bool:
        return (self.persistence_type == PersistenceType.MIXED
                or self.persistence_type == PersistenceType.AOF)

    def _need_ldb_persistence(self) -> bool:
        return (self.persistence_type == PersistenceType.MIXED
                or self.persistence_type == PersistenceType.LDB)

    def process_command_line(self, dbname: str, cmdline: str):
        result = self._execute_command_line(dbname, cmdline)

        self.aof.log_command(dbname, cmdline)

        return result

    def _execute_command_line(self, dbname, cmdline):
        db = self.dbmanager.get_or_create_db(dbname)
        ctx = CommandContext(db)
        command = parse_command_line_to_object(cmdline)
        result = command.execute(ctx)
        return result

    def replay_commands_from_aof(self):
        for dbname, cmdline in self.aof.load_commands():
            self._execute_command_line(dbname, cmdline)
