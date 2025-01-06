import time

from refactor2.core.command.base import CommandContext, ReadCommand, WriteCommand
from refactor2.core.persistence import LitedisDB


class SetCommand(WriteCommand):
    name = 'set'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.value: str
        self.options: dict

        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):

        if len(tokens) < 3:
            raise ValueError('set command requires key and value')

        key = tokens[1]
        value = tokens[2]
        options = {}

        lower_tokens = [t.lower() for t in tokens[3:]]
        for i, token in enumerate(lower_tokens):
            match token:
                case "nx":
                    options["nx"] = True
                case "xx":
                    options["xx"] = True
                case "get":
                    options["get"] = True
                case "keepttl":
                    options["keepttl"] = True
                case "ex":
                    now = int(time.time() * 1000)
                    seconds = int(lower_tokens[i + 1])
                    options["expiration"] = now + seconds * 1000
                case "px":
                    now = int(time.time() * 1000)
                    milliseconds = int(lower_tokens[i + 1])
                    options["expiration"] = now + milliseconds
                case "exat":
                    options["expiration"] = int(lower_tokens[i + 1]) * 1000
                case "pxat":
                    options["expiration"] = int(lower_tokens[i + 1])

        self.key = key
        self.value = value
        self.options = options

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

    def __init__(self, command_tokens: list[str]):
        self.key: str

        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('get command requires key')
        key = tokens[1]
        self.key = key

    def execute(self, ctx: CommandContext):
        return ctx.db.get(self.key)
