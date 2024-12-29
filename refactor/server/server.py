from pathlib import Path
from threading import Lock

from refactor.server.commands import COMMAND_CLASSES
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
            self.aof = AOF(self)
            self.aof.start_persistence_thread()

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
        command = self._create_command_from_strcmd(db, strcmd)
        result = command.execute()

        if self.aof:
            # todo 这里需要添加 command “读/写”标志，然后只记录写命令
            self.aof.append_command(db.dbname, strcmd)

        return result

    def _create_command_from_strcmd(self, db, strcmd):
        cmd_name, args = parse_string_command(strcmd)
        cmd_class = COMMAND_CLASSES.get(cmd_name)
        if cmd_class is None:
            raise ValueError(f'Unknown command "{cmd_name}"')
        command = cmd_class(db, cmd_name, args)
        return command
