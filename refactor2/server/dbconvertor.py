import time

from refactor2.server.persistence import LitedisDB


class DbCommandLineConverter:

    @classmethod
    def db_object_to_commands(cls, dbs: dict[str, LitedisDB]):
        for dbname, db in dbs.items():
            for key in db.keys():
                cmdline = cls._convert_db_object_to_cmdline(key, db)
                yield dbname, cmdline

    @classmethod
    def _convert_db_object_to_cmdline(cls, key: str, db: LitedisDB):
        value = db.get(key)
        if value is None:
            raise KeyError(f"'{key}' doesn't exist")
        match value:
            case str():
                pieces = ['set', f'"{key}"', f'"{value}"']
            case _:
                raise TypeError(f"the value type the key({key}) is not supported")

        expiration = db.get_expiration(key)
        if expiration is not None:
            if int(expiration) > time.time() * 1000:
                pieces.append('pxat')
                pieces.append(f'{expiration}')

        return ' '.join(pieces)
