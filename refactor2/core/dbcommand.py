import time
from typing import Iterable

from refactor2.core.command.base import CommandContext
from refactor2.core.command.factory import CommandFactory
from refactor2.core.command.sortedset import SortedSet
from refactor2.core.persistence import LitedisDB
from refactor2.typing import DBCommandPair


class DBCommandConverter:

    @classmethod
    def dbs_to_commands(cls, dbs: dict[str, LitedisDB]):
        for dbname, db in dbs.items():
            for key in db.keys():
                cmdtokens = cls._convert_db_object_to_cmdtokens(key, db)
                yield DBCommandPair(dbname, cmdtokens)

    @classmethod
    def _convert_db_object_to_cmdtokens(cls, key: str, db: LitedisDB):
        value = db.get(key)
        if value is None:
            raise KeyError(f"'{key}' doesn't exist")
        match value:
            case str():
                pieces = ['set', key, value]
            case dict():
                pieces = ['hset', key]
                for field, val in value.items():
                    pieces.extend([field, str(val)])
            case list():
                pieces = ['rpush', key, *value]
            case set():
                pieces = ['sadd', key, *value]
            case SortedSet():
                pieces = ['zadd', key]
                for member, score in value.items():
                    pieces.extend([str(score), member])
            case _:
                raise TypeError(f"the value type the key({key}) is not supported")

        expiration = db.get_expiration(key)
        if expiration is not None:
            if int(expiration) > time.time() * 1000:
                pieces.append('pxat')
                pieces.append(f'{expiration}')

        return pieces

    @classmethod
    def commands_to_dbs(cls, dbcmds: Iterable[DBCommandPair]) -> dict[str, LitedisDB]:
        dbs = {}
        for dbcmd in dbcmds:
            dbname, cmdtokens = dbcmd

            db = dbs.get(dbname)
            if db is None:
                db = LitedisDB(dbname)
                dbs[dbname] = db

            ctx = CommandContext(db)
            command = CommandFactory.create(cmdtokens)
            command.execute(ctx)

        return dbs
