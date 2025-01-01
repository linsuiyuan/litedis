from pathlib import Path
from threading import Lock

from refactor2.server.interfaces import CommandLogger
from refactor2.server.persistence import LitedisDB
from refactor2.server.persistence import AOF
from refactor2.typing import PersistenceType
from refactor2.utils import thread_safe_singleton

_dbs = {}
_dbs_lock = Lock()


@thread_safe_singleton
class DBManager(CommandLogger):

    def __init__(self,
                 data_path: str | Path = "ldbdata",
                 persistence_type=PersistenceType.MIXED):

        self.persistence_type = persistence_type

        self.data_path = data_path if isinstance(data_path, Path) else Path(data_path)
        self.data_path.mkdir(parents=True, exist_ok=True)

        self.ldb_filename = "litedis.ldb"
        self.ldb_filepath = self.data_path / self.ldb_filename

        self.aof: AOF | None = None

    def _need_aof_persistence(self) -> bool:
        return (self.persistence_type == PersistenceType.MIXED
                or self.persistence_type == PersistenceType.AOF)

    def _need_ldb_persistence(self) -> bool:
        return (self.persistence_type == PersistenceType.MIXED
                or self.persistence_type == PersistenceType.LDB)

    def get_or_create_db(self, dbname):
        if dbname not in _dbs:
            self._create_db(dbname)
        return _dbs[dbname]

    def _create_db(self, dbname):
        with _dbs_lock:
            if dbname not in _dbs:
                _dbs[dbname] = LitedisDB(dbname)

    def log_command(self, dbname: str, cmdline: str):
        if self.aof:
            self.aof.log_command(dbname, cmdline)
