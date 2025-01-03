import time
from pathlib import Path
from threading import Lock, Thread

from refactor2.server.commands import CommandContext
from refactor2.server.commands.parsers import parse_command_line_to_object
from refactor2.server.dbcommand import DBCommandLineConverter, DBCommandLine
from refactor2.server.persistence import AOF
from refactor2.server.persistence import LitedisDB
from refactor2.typing import CommandLogger, CommandProcessor
from refactor2.utils import SingletonMeta

_dbs: dict[str, LitedisDB] = {}
_dbs_lock = Lock()


class DBManager(CommandProcessor, metaclass=SingletonMeta):
    aof: AOF | None = None
    # if less than or equal 0, means shouldn't rewrite
    aof_rewrite_cycle = 666
    _ldb_filename = "litedis.ldb"

    command_logger: CommandLogger | None = None

    def __init__(self,
                 data_path: str | Path = "ldbdata"):
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)
        self.data_path.mkdir(parents=True, exist_ok=True)

        self._load_aof_data()
        self._start_aof_rewrite_loop()

    @property
    def _ldb_filepath(self) -> Path:
        return self.data_path / self._ldb_filename

    def _load_aof_data(self):
        self.aof = AOF(self.data_path)
        self.command_logger = self.aof

        self._replay_aof_commands()

    def _start_aof_rewrite_loop(self):
        if not self.aof:
            return False

        if not self.aof.exists_file():
            return False

        if self.aof_rewrite_cycle <= 0:
            return False

        self._rewrite_aof_commands()
        self._rewrite_aof_loop()

    def _rewrite_aof_loop(self):
        def loop():
            while True:
                time.sleep(self.aof_rewrite_cycle)
                self._rewrite_aof_commands()
        thread = Thread(target=loop, daemon=True)
        thread.start()

    def get_or_create_db(self, dbname):
        if dbname not in _dbs:
            with _dbs_lock:
                if dbname not in _dbs:
                    _dbs[dbname] = LitedisDB(dbname)
        return _dbs[dbname]

    def process_command(self, dbcmd: DBCommandLine):
        db = self.get_or_create_db(dbcmd.dbname)
        ctx = CommandContext(db)
        command = parse_command_line_to_object(dbcmd.cmdline)
        result = command.execute(ctx)

        # todo lock if needed
        if self.command_logger:
            self.command_logger.log_command(dbcmd)

        return result

    def _replay_aof_commands(self) -> bool:
        if not self.aof.exists_file():
            return False

        with _dbs_lock:
            global _dbs
            dbcmds = self.aof.load_commands()
            _dbs = DBCommandLineConverter.commands_to_dbs(dbcmds)

        return True

    def _rewrite_aof_commands(self) -> bool:

        with _dbs_lock:
            dbcommands = DBCommandLineConverter.dbs_to_commands(_dbs)
            self.aof.rewrite_commands(dbcommands)

        return True
