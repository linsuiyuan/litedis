import time
from typing import Iterable

from refactor2.commandline import combine_command_line
from refactor2.core.command.commands import CommandContext
from refactor2.core.command.parsers import parse_command_line_to_command
from refactor2.core.persistence import LitedisDB
from refactor2.typing import DBCommandLine


class DBCommandLineConverter:

    @classmethod
    def dbs_to_commands(cls, dbs: dict[str, LitedisDB]):
        for dbname, db in dbs.items():
            for key in db.keys():
                cmdline = cls._convert_db_object_to_cmdline(key, db)
                yield DBCommandLine(dbname, cmdline)

    @classmethod
    def _convert_db_object_to_cmdline(cls, key: str, db: LitedisDB):
        value = db.get(key)
        if value is None:
            raise KeyError(f"'{key}' doesn't exist")
        match value:
            case str():
                pieces = ['set', key, value]
            case _:
                raise TypeError(f"the value type the key({key}) is not supported")

        expiration = db.get_expiration(key)
        if expiration is not None:
            if int(expiration) > time.time() * 1000:
                pieces.append('pxat')
                pieces.append(f'{expiration}')

        return combine_command_line(pieces)

    @classmethod
    def commands_to_dbs(cls, dbcmds: Iterable[DBCommandLine]) -> dict[str, LitedisDB]:
        dbs = {}
        for dbcmd in dbcmds:
            dbname, cmdline = dbcmd

            db = dbs.get(dbname)
            if db is None:
                db = LitedisDB(dbname)
                dbs[dbname] = db

            ctx = CommandContext(db)
            command = parse_command_line_to_command(dbcmd.cmdline)
            command.execute(ctx)

        return dbs
