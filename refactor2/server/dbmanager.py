from pathlib import Path
from threading import Lock

from refactor2.server.commands import CommandContext
from refactor2.server.commands.parsers import parse_command_line_to_object
from refactor2.server.persistence import AOF
from refactor2.server.persistence import LitedisDB
from refactor2.typing import PersistenceType, CommandLogger, CommandProcessor
from refactor2.utils import SingletonMeta

_dbs = {}
_dbs_lock = Lock()


class DBManager(CommandProcessor, metaclass=SingletonMeta):
    aof: AOF | None = None
    _ldb_filename = "litedis.ldb"

    command_logger: CommandLogger | None = None

    def __init__(self,
                 data_path: str | Path = "ldbdata",
                 persistence_type=PersistenceType.MIXED):
        self.persistence_type = persistence_type
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)
        self.data_path.mkdir(parents=True, exist_ok=True)
        self._load_data()

    @property
    def _ldb_filepath(self) -> Path:
        return self.data_path / self._ldb_filename

    @property
    def _need_aof_persistence(self) -> bool:
        return (self.persistence_type == PersistenceType.MIXED
                or self.persistence_type == PersistenceType.AOF)

    @property
    def _need_ldb_persistence(self) -> bool:
        return (self.persistence_type == PersistenceType.MIXED
                or self.persistence_type == PersistenceType.LDB)

    def _load_data(self):
        if self._need_aof_persistence:
            self.aof = AOF(self.data_path)
            self.command_logger = self.aof
            self._load_aof_data()

    def get_or_create_db(self, dbname):
        if dbname not in _dbs:
            self._create_db(dbname)
        return _dbs[dbname]

    def _create_db(self, dbname):
        with _dbs_lock:
            if dbname not in _dbs:
                _dbs[dbname] = LitedisDB(dbname)

    def _load_aof_data(self) -> bool:
        if not self.aof.exists_file():
            print("aof file does not exist")
            return False

        for dbname, cmdline in self.aof.load_commands():
            self.replay_command(dbname, cmdline)

        return True

    def process_command(self, dbname: str, cmdline: str):
        result = self._execute_command_line(dbname, cmdline)

        if self.command_logger:
            self.command_logger.log_command(dbname, cmdline)

        return result

    def replay_command(self, dbname: str, cmdline: str):
        self._execute_command_line(dbname, cmdline)

    def _execute_command_line(self, dbname: str, cmdline: str):
        db = self.get_or_create_db(dbname)
        ctx = CommandContext(db)
        command = parse_command_line_to_object(cmdline)
        return command.execute(ctx)
