from threading import Lock

from refactor2.server.persistence.ldb import LitedisDB
from refactor2.utils import thread_safe_singleton

_dbs = {}
_dbs_lock = Lock()


@thread_safe_singleton
class DBManager:

    def get_or_create_db(self, dbname):
        if dbname not in _dbs:
            self._create_db(dbname)
        return _dbs[dbname]

    def _create_db(self, dbname):
        with _dbs_lock:
            if dbname not in _dbs:
                _dbs[dbname] = LitedisDB(dbname)
