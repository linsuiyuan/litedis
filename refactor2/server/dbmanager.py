import time
from pathlib import Path
from threading import Lock, Thread

from refactor2.server.commands import CommandContext
from refactor2.server.commands.parsers import parse_command_line_to_object
from refactor2.server.dbcommand import DBCommandLineConverter, DBCommandLine
from refactor2.server.persistence import AOF
from refactor2.server.persistence import LitedisDB
from refactor2.typing import CommandProcessor, ReadWriteType
from refactor2.utils import SingletonMeta

_dbs: dict[str, LitedisDB] = {}
_dbs_lock = Lock()


class DBManager(CommandProcessor, metaclass=SingletonMeta):

    def __init__(self,
                 persistence_on=True,
                 data_path: str | Path = Path("ldbdata"),
                 aof_rewrite_cycle = 666):
        if not persistence_on:
            return
        self._persistence_on = persistence_on

        self._data_path = data_path if isinstance(data_path, Path) else Path(data_path)
        self._data_path.mkdir(parents=True, exist_ok=True)

        self._aof_rewrite_cycle = aof_rewrite_cycle

        self._aof: AOF | None = None

        self._load_aof_data()
        self._start_aof_rewrite_loop()

    def _load_aof_data(self):
        self._aof = AOF(self._data_path)

        self._replay_aof_commands()

    def _start_aof_rewrite_loop(self):
        if not self._aof:
            return False

        if not self._aof.exists_file():
            return False

        if self._aof_rewrite_cycle <= 0:
            return False

        self._rewrite_aof_commands()
        self._rewrite_aof_loop()

    def _rewrite_aof_loop(self):
        def loop():
            while True:
                time.sleep(self._aof_rewrite_cycle)
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

        # todo lock if needed
        result = command.execute(ctx)

        if self._persistence_on and self._aof:
            if command.rwtype == ReadWriteType.Write:
                self._aof.log_command(dbcmd)

        return result

    def _replay_aof_commands(self) -> bool:
        if not self._aof.exists_file():
            return False

        with _dbs_lock:
            global _dbs
            dbcmds = self._aof.load_commands()
            _dbs = DBCommandLineConverter.commands_to_dbs(dbcmds)

        return True

    def _rewrite_aof_commands(self) -> bool:

        with _dbs_lock:
            dbcommands = DBCommandLineConverter.dbs_to_commands(_dbs)
            self._aof.rewrite_commands(dbcommands)

        return True
