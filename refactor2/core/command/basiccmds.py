import random
import re
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

        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        return ctx.db.get(self.key)


class AppendCommand(WriteCommand):
    name = 'append'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.value: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('append command requires key and value')
        self.key = tokens[1]
        self.value = tokens[2]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            db.set(self.key, self.value)
            return len(self.value)

        old_value = db.get(self.key)
        if not isinstance(old_value, str):
            raise TypeError("value is not a string")

        new_value = old_value + self.value
        db.set(self.key, new_value)
        return len(new_value)


class DecrbyCommand(WriteCommand):
    name = 'decrby'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.decrement: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('decrby command requires key and decrement')
        self.key = tokens[1]
        try:
            self.decrement = int(tokens[2])
        except ValueError:
            raise ValueError('decrement must be an integer')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            value = 0
        else:
            value = db.get(self.key)
            if not isinstance(value, str):
                raise TypeError("value is not a string")
            try:
                value = int(value)
            except ValueError:
                raise ValueError("value is not an integer")

        new_value = str(value - self.decrement)
        db.set(self.key, new_value)
        return new_value


class DeleteCommand(WriteCommand):
    name = 'del'

    def __init__(self, command_tokens: list[str]):
        self.keys: list[str]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('del command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        deleted = 0
        for key in self.keys:
            deleted += ctx.db.delete(key)
        return deleted


class ExistsCommand(ReadCommand):
    name = 'exists'

    def __init__(self, command_tokens: list[str]):
        self.keys: list[str]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('exists command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        count = 0
        for key in self.keys:
            if ctx.db.exists(key):
                count += 1
        return count


class CopyCommand(WriteCommand):
    name = 'copy'

    def __init__(self, command_tokens: list[str]):
        self.source: str
        self.destination: str
        self.replace: bool
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('copy command requires source and destination')
        self.source = tokens[1]
        self.destination = tokens[2]
        self.replace = False

        if len(tokens) > 3:
            if tokens[3].lower() == 'replace':
                self.replace = True

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.source):
            return 0

        if db.exists(self.destination) and not self.replace:
            return 0

        value = db.get(self.source)
        db.set(self.destination, value)

        # Copy expiration if exists
        if db.exists_expiration(self.source):
            expiration = db.get_expiration(self.source)
            db.set_expiration(self.destination, expiration)

        return 1


class ExpireCommand(WriteCommand):
    name = 'expire'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.seconds: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('expire command requires key and seconds')
        self.key = tokens[1]
        try:
            self.seconds = int(tokens[2])
        except ValueError:
            raise ValueError('seconds must be an integer')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        now = int(time.time() * 1000)
        expiration = now + self.seconds * 1000
        return db.set_expiration(self.key, expiration)


class ExpireatCommand(WriteCommand):
    name = 'expireat'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.timestamp: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('expireat command requires key and timestamp')
        self.key = tokens[1]
        try:
            self.timestamp = int(tokens[2])
        except ValueError:
            raise ValueError('timestamp must be an integer')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        # Convert seconds to milliseconds
        expiration = self.timestamp * 1000
        return db.set_expiration(self.key, expiration)


class ExpireTimeCommand(ReadCommand):
    name = 'expiretime'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('expiretime command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        expiration = db.get_expiration(self.key)
        if expiration == -2:  # Key does not exist
            return -2
        if expiration == -1:  # Key exists but has no expiration
            return -1
        # Convert milliseconds to seconds
        return expiration // 1000


class IncrbyCommand(WriteCommand):
    name = 'incrby'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.increment: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('incrby command requires key and increment')
        self.key = tokens[1]
        try:
            self.increment = int(tokens[2])
        except ValueError:
            raise ValueError('increment must be an integer')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            value = 0
        else:
            value = db.get(self.key)
            if not isinstance(value, str):
                raise TypeError("value is not a string")
            try:
                value = int(value)
            except ValueError:
                raise ValueError("value is not an integer")

        new_value = str(value + self.increment)
        db.set(self.key, new_value)
        return new_value


class IncrbyfloatCommand(WriteCommand):
    name = 'incrbyfloat'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.increment: float
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('incrbyfloat command requires key and increment')
        self.key = tokens[1]
        try:
            self.increment = float(tokens[2])
        except ValueError:
            raise ValueError('increment must be a float')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            value = 0.0
        else:
            value = db.get(self.key)
            if not isinstance(value, str):
                raise TypeError("value is not a string")
            try:
                value = float(value)
            except ValueError:
                raise ValueError("value is not a float")

        new_value = str(value + self.increment)
        # Remove trailing zeros and decimal point if it's a whole number
        if '.' in new_value:
            new_value = new_value.rstrip('0').rstrip('.')

        db.set(self.key, new_value)
        return new_value


class KeysCommand(ReadCommand):
    name = 'keys'

    def __init__(self, command_tokens: list[str]):
        self.pattern: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('keys command requires pattern')
        self.pattern = tokens[1]

    def _convert_pattern_to_regex(self, pattern: str) -> str:
        """Convert Redis glob pattern to Python regex pattern

        Handles the following Redis wildcards:
        * - matches any sequence of characters
        ? - matches any single character
        [] - matches any character within the brackets
        \\x - escape character x
        """
        # Handle escaped characters first
        i = 0
        result = []
        while i < len(pattern):
            if pattern[i] == '\\' and i + 1 < len(pattern):
                result.append(re.escape(pattern[i + 1]))
                i += 2
            else:
                result.append(pattern[i])
                i += 1
        pattern = ''.join(result)

        # Convert Redis wildcards to regex
        pattern = pattern.replace('*', '.*')  # * -> .*
        pattern = pattern.replace('?', '.')  # ? -> .

        # Add anchors to match entire string
        return f'^{pattern}$'

    def execute(self, ctx: CommandContext):
        try:
            # Convert Redis pattern to regex pattern
            regex_pattern = self._convert_pattern_to_regex(self.pattern)
            pattern = re.compile(regex_pattern)

            # Filter keys using the pattern
            matched_keys = []
            for key in ctx.db.keys():
                if pattern.match(key):
                    matched_keys.append(key)

            return matched_keys

        except re.error as e:
            raise ValueError(f"Invalid pattern: {str(e)}")


class MgetCommand(ReadCommand):
    name = 'mget'

    def __init__(self, command_tokens: list[str]):
        self.keys: list[str]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('mget command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        result = []
        for key in self.keys:
            result.append(db.get(key))
        return result


class MsetCommand(WriteCommand):
    name = 'mset'

    def __init__(self, command_tokens: list[str]):
        self.pairs: list[tuple[str, str]]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3 or len(tokens) % 2 != 1:
            raise ValueError('mset command requires key value pairs')

        # Convert flat list to pairs
        self.pairs = []
        for i in range(1, len(tokens), 2):
            self.pairs.append((tokens[i], tokens[i + 1]))

    def execute(self, ctx: CommandContext):
        db = ctx.db
        for key, value in self.pairs:
            db.set(key, value)
        return "OK"


class MsetnxCommand(WriteCommand):
    name = 'msetnx'

    def __init__(self, command_tokens: list[str]):
        self.pairs: list[tuple[str, str]]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3 or len(tokens) % 2 != 1:
            raise ValueError('msetnx command requires key value pairs')

        # Convert flat list to pairs
        self.pairs = []
        for i in range(1, len(tokens), 2):
            self.pairs.append((tokens[i], tokens[i + 1]))

    def execute(self, ctx: CommandContext):
        db = ctx.db

        # First check if any key exists
        for key, _ in self.pairs:
            if db.exists(key):
                return 0

        # If none exist, set all of them
        for key, value in self.pairs:
            db.set(key, value)
        return 1


class PersistCommand(WriteCommand):
    name = 'persist'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('persist command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        # If key has no expiration, return 0
        if not db.exists_expiration(self.key):
            return 0

        # Remove expiration and return 1
        db.delete_expiration(self.key)
        return 1


class RandomKeyCommand(ReadCommand):
    name = 'randomkey'

    def __init__(self, command_tokens: list[str]):
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) > 1:
            raise ValueError('randomkey command takes no arguments')

    def execute(self, ctx: CommandContext):
        keys = list(ctx.db.keys())
        if not keys:
            return None
        return random.choice(keys)


class RenameCommand(WriteCommand):
    name = 'rename'

    def __init__(self, command_tokens: list[str]):
        self.source: str
        self.destination: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError(f'{self.name} command requires source and destination')
        self.source = tokens[1]
        self.destination = tokens[2]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if self.source == self.destination:
            raise ValueError("source and destination keys are the same")
        if not db.exists(self.source):
            raise ValueError("source key does not exist")

        # Get the value and any expiration from source
        value = db.get(self.source)
        expiration = None
        if db.exists_expiration(self.source):
            expiration = db.get_expiration(self.source)

        # Delete the source key
        db.delete(self.source)

        # Set the destination key
        db.set(self.destination, value)
        if expiration is not None:
            db.set_expiration(self.destination, expiration)

        return "OK"


class RenamenxCommand(RenameCommand):
    name = 'renamenx'

    def execute(self, ctx: CommandContext):
        if ctx.db.exists(self.destination):
            return 0

        super().execute(ctx)

        return 1


class StrlenCommand(ReadCommand):
    name = 'strlen'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('strlen command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get(self.key)
        if not isinstance(value, str):
            raise TypeError("value is not a string")

        return len(value)


class SubstrCommand(ReadCommand):
    name = 'substr'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.start: int
        self.end: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 4:
            raise ValueError('substr command requires key, start and end')
        self.key = tokens[1]
        try:
            self.start = int(tokens[2])
            self.end = int(tokens[3])
        except ValueError:
            raise ValueError('start and end must be integers')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return None

        value = db.get(self.key)
        if not isinstance(value, str):
            raise TypeError("value is not a string")

        # Handle negative indices
        start, end = self.start, self.end
        length = len(value)
        if start < 0:
            start = length + start
        if end < 0:
            end = length + end

        # Ensure start and end are within bounds
        start = max(0, min(start, length))
        end = max(0, min(end + 1, length))  # +1 because Redis is inclusive of end

        return value[start:end]


class TTLCommand(ReadCommand):
    name = 'ttl'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('ttl command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db

        expiration = db.get_expiration(self.key)
        if expiration == -2:
            return -2  # Key does not exist
        if expiration == -1:
            return -1  # Key exists but has no associated expire

        now = int(time.time() * 1000)
        remaining = expiration - now

        # Return remaining time in seconds, rounded down
        return max(remaining // 1000, 0)


class PTTLCommand(ReadCommand):
    name = 'pttl'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('pttl command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db

        expiration = db.get_expiration(self.key)
        if expiration == -2:
            return -2  # Key does not exist
        if expiration == -1:
            return -1  # Key exists but has no associated expire

        now = int(time.time() * 1000)
        remaining = expiration - now

        return max(remaining, 0)


class TypeCommand(ReadCommand):
    name = 'type'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('type command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return "none"

        return db.get_type(self.key)


class UnlinkCommand(WriteCommand):
    name = 'unlink'

    def __init__(self, command_tokens: list[str]):
        self.keys: list[str]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('unlink command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        deleted = 0
        for key in self.keys:
            deleted += ctx.db.delete(key)
        return deleted

