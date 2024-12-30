from pathlib import Path
from threading import Lock

from refactor.server import LitedisDb
from refactor.server.commands import create_command_from_strcmd
from refactor.typing import PersistenceType
from refactor.utils import thread_safe_singleton

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

        return result
