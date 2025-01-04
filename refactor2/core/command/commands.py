from abc import ABC, abstractmethod

from refactor2.core.persistence import LitedisDB
from refactor2.typing import ReadWriteType


class CommandContext:

    def __init__(self, db: LitedisDB, attrs: dict = None):
        self.db = db
        self.attrs = {} if attrs is None else attrs


class Command(ABC):
    name = None

    @property
    @abstractmethod
    def rwtype(self) -> ReadWriteType: ...

    @abstractmethod
    def execute(self, ctx: CommandContext): ...


class ReadCommand(Command, ABC):
    @property
    def rwtype(self) -> ReadWriteType:
        return ReadWriteType.Read


class WriteCommand(Command, ABC):
    @property
    def rwtype(self) -> ReadWriteType:
        return ReadWriteType.Write


class SetCommand(WriteCommand):
    name = 'set'

    def __init__(self, key, value, options=None):
        self.key = key
        self.value = value
        self.options = {} if options is None else options

    def execute(self, ctx: CommandContext):
        self._check_that_nx_and_xx_cannot_both_be_set()

        db = ctx.db

        if self._is_nx_set_and_key_exists(db):
            return None

        if self._is_xx_set_and_key_not_exists(db):
            return None

        old_value = db.get(self.key)

        db.set(self.key, self.value)

        if not self.options.get('keepttl'):
            db.delete_expiration(self.key)

        if expiration := self.options.get('expiration'):
            db.set_expiration(self.key, expiration)

        if self.options.get("get"):
            return old_value

        return "OK"

    def _check_that_nx_and_xx_cannot_both_be_set(self):
        if self.options.get('nx') and self.options.get('xx'):
            raise ValueError("nx and xx are mutually exclusive")

    def _is_nx_set_and_key_exists(self, db: LitedisDB):
        if self.options.get('nx') and db.exists(self.key):
            return True

    def _is_xx_set_and_key_not_exists(self, db: LitedisDB):
        if self.options.get('xx') and not db.exists(self.key):
            return True


class GetCommand(ReadCommand):
    name = 'get'

    def __init__(self, key):
        self.key = key

    def execute(self, ctx: CommandContext):
        return ctx.db.get(self.key)
