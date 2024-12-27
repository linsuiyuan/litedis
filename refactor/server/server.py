from threading import Lock

from refactor.server.db import LitedisDb
from refactor.utils import thread_safe_singleton

_dbs = {}
_dbs_lock = Lock()

@thread_safe_singleton
class LitedisServer:

    def get_or_create_db(self, id_):
        if id_ not in _dbs:
            self._create_db(id_)
        return _dbs[id_]

    def _create_db(self, id_):
        with _dbs_lock:
            if id_ not in _dbs:
                _dbs[id_] = LitedisDb(id_)

    def exists_db(self, id_):
        return id_ in _dbs

    def close_db(self, id_):
        with _dbs_lock:
            if id_ in _dbs:
                del _dbs[id_]

