from abc import ABC, abstractmethod

from refactor.server import LitedisDb
from refactor.typing import StringLikeT, LitedisObjectT


class Command(ABC):
    name = None

    def __init__(self, db: LitedisDb, name: str, args: list[StringLikeT], raw_cmd: str):
        self.db = db
        self.name = name
        self.args = args
        self.raw_cmd = raw_cmd

        self._check_args_count()

    @abstractmethod
    def _check_args_count(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    def _set_default_if_key_not_exists(self, key, default: LitedisObjectT):
        if key not in self.db:
            self.db.set(key, default)


