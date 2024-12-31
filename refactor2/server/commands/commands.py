from abc import ABC, abstractmethod

from refactor2.server.commands import CommandExecutionContext
from refactor2.server.persistence import LitedisDB


class Command(ABC):
    name = None

    @abstractmethod
    def execute(self, ctx: CommandExecutionContext):...


class SetCommand(Command):
    name = 'set'

    def __init__(self, key, value, options=None):
        self.key = key
        self.value = value
        self.options = {} if options is None else options


    def execute(self, ctx: CommandExecutionContext):
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



