from pathlib import Path
from threading import Lock

from refactor.server.commands import COMMAND_CLASSES, create_command_from_strcmd
from refactor.server import LitedisDb, AOF
from refactor.typing import PersistenceType
from refactor.utils import parse_string_command, thread_safe_singleton

_dbs = {}
_dbs_lock = Lock()


@thread_safe_singleton
class LitedisServer:
    def __init__(
            self,
            data_path: str | Path = "ldbdata",
            persistence: PersistenceType = "mixed",
            ldb_save_frequency: int = 600
    ):
        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)
        self.persistence = persistence
        self.ldb_save_frequency = ldb_save_frequency

        self.data_path.mkdir(parents=True, exist_ok=True)

        self._init_aof()

    def _init_aof(self):
        if self._is_aof_persistence_needed():
            self.aof = AOF(self.data_path)
            self.aof.start()

    def _is_aof_persistence_needed(self):
        return self.persistence == "aof" or self.persistence == "mixed"

    def get_or_create_db(self, dbname):
        if dbname not in _dbs:
            self._create_db(dbname)
        return _dbs[dbname]

    def _create_db(self, dbname):
        with _dbs_lock:
            if dbname not in _dbs:
                _dbs[dbname] = LitedisDb(dbname)

    def exists_db(self, dbname):
        return dbname in _dbs

    def close_db(self, dbname):
        with _dbs_lock:
            if dbname in _dbs:
                del _dbs[dbname]

    def process_command(self, db: LitedisDb, strcmd: str):
        command = create_command_from_strcmd(db, strcmd)
        result = command.execute()

        if self.aof:
            # todo 这里需要添加 command “读/写”标志，然后只记录写命令
            self.aof.append_command(command)

        return result

