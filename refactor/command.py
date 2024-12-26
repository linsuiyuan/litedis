import time
from abc import ABC, abstractmethod
from functools import lru_cache

from refactor.db import LitedisDb
from refactor.typing import StringLikeT, KeyT, LitedisObjectT

class Command(ABC):
    def __init__(self, db: LitedisDb, name: str, args: list[StringLikeT]):
        self.db = db
        self.name = name
        self.args = args

        self._check_args_count()

    @abstractmethod
    def _check_args_count(self):
        pass

    @abstractmethod
    def execute(self):
        pass

class SetCommand(Command):

    def _check_args_count(self):
        if len(self.args) < 2:
            raise ValueError(f"SetCommand takes more than 1 arguments, {len(self.args)} given")

    def execute(self):
        if self.nx and self.xx:
            raise ValueError("nx and xx cannot be set at the same time")
        if self.nx and self.key in self.db:
            return None
        if self.xx and self.key not in self.db:
            return None

        self.db.set(self.key, self.value)

        self.db.delete_expiration(self.key)
        if self.expiration:
            self.db.set_expiration(self.key, self.expiration)

        return "OK"

    def _lower_args_omit_first_two(self) -> list[StringLikeT]:
        return [s.lower() if isinstance(s, str) else s
                for s in self.args[2:]]

    @property
    def key(self) -> KeyT:
        return self.args[0]

    @property
    def value(self) -> LitedisObjectT:
        return self.args[1]

    @property
    def nx(self) -> bool:
        lower_args = self._lower_args_omit_first_two()
        return "nx" in lower_args

    @property
    def xx(self) -> bool:
        lower_args = self._lower_args_omit_first_two()
        return "xx" in lower_args

    @property
    @lru_cache
    def expiration(self) -> int:
        lower_args = self._lower_args_omit_first_two()
        if len(lower_args) < 2:
            return 0

        if "ex" in lower_args:
            expiration = self._get_ex_milli_timestamp(lower_args)
        elif "px" in lower_args:
            expiration = self._get_px_milli_timestamp(lower_args)
        elif "exat" in lower_args:
            expiration = self._get_exat_milli_timestamp(lower_args)
        elif "pxat" in lower_args:
            expiration = self._get_pxat_milli_timestamp(lower_args)
        else:
            expiration = 0

        return expiration

    def _get_ex_milli_timestamp(self, args) -> int:
        index = args.index("ex")
        now = int(time.time() * 1000)
        return now + args[index + 1] * 1000

    def _get_px_milli_timestamp(self, args) -> int:
        index = args.index("px")
        now = int(time.time() * 1000)
        return now + args[index + 1]

    def _get_exat_milli_timestamp(self, args) -> int:
        index = args.index("exat")
        return args[index + 1] * 1000

    def _get_pxat_milli_timestamp(self, args) -> int:
        index = args.index("pxat")
        return args[index + 1]

class GetCommand(Command):

    def _check_args_count(self):
        if len(self.args) < 1:
            raise ValueError(f"GetCommand requires at least 1 argument, {len(self.args)} given")

    def execute(self):
        return self.db.get(self.key)

    @property
    def key(self) -> KeyT:
        return self.args[0]
