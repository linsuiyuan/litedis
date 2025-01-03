from copy import deepcopy
from pathlib import Path
from threading import Lock

from refactor2.server.commands import CommandContext
from refactor2.server.commands.parsers import parse_command_line_to_object
from refactor2.server.dbcommand import DBCommandLineConverter, DBCommandLine
from refactor2.server.persistence import AOF
from refactor2.server.persistence import LitedisDB
from refactor2.typing import PersistenceType, CommandLogger, CommandProcessor
from refactor2.utils import SingletonMeta

_dbs: dict[str, LitedisDB] = {}
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

            result = self._replay_aof_commands()

            if result:
                self._rewrite_aof_commands()

    def get_or_create_db(self, dbname):
        if dbname not in _dbs:
            self._create_db(dbname)
        return _dbs[dbname]

    def _create_db(self, dbname):
        with _dbs_lock:
            if dbname not in _dbs:
                _dbs[dbname] = LitedisDB(dbname)

    def process_command(self, dbcmd: DBCommandLine):
        result = self._execute_command_line(dbcmd)

        if self.command_logger:
            self.command_logger.log_command(dbcmd)

        return result

    def _execute_command_line(self, dbcmd: DBCommandLine):
        db = self.get_or_create_db(dbcmd.dbname)
        ctx = CommandContext(db)
        command = parse_command_line_to_object(dbcmd.cmdline)
        return command.execute(ctx)

    def _replay_aof_commands(self):
        if not self.aof.exists_file():
            print("aof file does not exist")
            return False

        dbcmds = self.aof.load_commands()
        dbs = DBCommandLineConverter.commands_to_dbs(dbcmds)
        with _dbs_lock:
            global _dbs
            _dbs = dbs

        return True

    def _rewrite_aof_commands(self):
        with _dbs_lock:
            dbs = deepcopy(_dbs)

        dbcommands = DBCommandLineConverter.dbs_to_commands(dbs)
        self.aof.rewrite_commands(dbcommands)



