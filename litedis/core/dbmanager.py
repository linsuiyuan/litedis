import time
from collections import defaultdict
from pathlib import Path
from threading import Lock, Thread
from typing import Union, Optional, Dict

from litedis.core.command.base import CommandContext
from litedis.core.command.factory import CommandFactory
from litedis.core.dbcommand import DBCommandConverter, DBCommandPair
from litedis.core.persistence import AOF
from litedis.core.persistence import LitedisDB
from litedis.typing import CommandProcessor, ReadWriteType
from litedis.utils import SingletonMeta


class DBManager(CommandProcessor, metaclass=SingletonMeta):
    _dbs: Dict[str, LitedisDB] = {}
    _dbs_lock = Lock()
    _db_locks = defaultdict(Lock)

    def __init__(self,
                 data_path: Union[str, Path] = Path("ldbdata"),
                 persistence_on=True,
                 aof_rewrite_cycle=666):
        self.persistence_on = persistence_on
        if not self.persistence_on:
            return

        self._data_path = data_path if isinstance(data_path, Path) else Path(data_path)
        self._data_path.mkdir(parents=True, exist_ok=True)

        self._aof_rewrite_cycle = aof_rewrite_cycle

        self._aof: Optional[AOF] = None
        self._aof_lock = Lock()

        self._load_aof_data()
        self._start_aof_rewrite_loop()

    def _load_aof_data(self):
        self._aof = AOF(self._data_path)

        self._replay_aof_commands()

    def _start_aof_rewrite_loop(self):
        if not self._aof:
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
        if dbname not in self._dbs:
            with self._dbs_lock:
                if dbname not in self._dbs:
                    self._dbs[dbname] = LitedisDB(dbname)
        return self._dbs[dbname]

    def process_command(self, dbcmd: DBCommandPair):
        db = self.get_or_create_db(dbcmd.dbname)
        ctx = CommandContext(db, dbcmd.cmdtokens)
        command = CommandFactory.create(dbcmd.cmdtokens[0])

        with self._db_locks[dbcmd.dbname]:
            result = command.execute(ctx)

        if self.persistence_on and self._aof:
            if command.rwtype == ReadWriteType.Write:
                with self._aof_lock:
                    self._aof.log_command(dbcmd)

        return result

    def _replay_aof_commands(self) -> bool:
        if not self._aof.exists_file():
            return False

        with self._dbs_lock:
            dbcmds = self._aof.load_commands()
            dbs = DBCommandConverter.commands_to_dbs(dbcmds)
            self._dbs.clear()
            self._dbs.update(dbs)

        return True

    def _rewrite_aof_commands(self) -> bool:

        with self._dbs_lock:
            dbcommands = DBCommandConverter.dbs_to_commands(self._dbs)
            self._aof.rewrite_commands(dbcommands)

        return True
