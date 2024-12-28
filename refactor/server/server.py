from threading import Lock

from refactor.server.commands import COMMAND_CLASSES
from refactor.server.db import LitedisDb
from refactor.utils import parse_string_command

_dbs = {}
_dbs_lock = Lock()


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

    def process_command(self, db: LitedisDb, strcmd: str):
        cmd_name, args = parse_string_command(strcmd)
        cmd_class = COMMAND_CLASSES.get(cmd_name)
        if cmd_class is None:
            raise ValueError(f'Unknown command "{cmd_name}"')

        command = cmd_class(db, cmd_name, args)
        return command.execute()
