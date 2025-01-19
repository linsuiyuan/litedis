import random
import re
import time
from typing import Optional, List, Tuple

from litedis.core.command.base import CommandContext, ReadCommand, WriteCommand


class SetCommand(WriteCommand):
    name = 'set'
    __slots__ = ('key', 'value', 'ex', 'px', 'exat', 'pxat', 'nx', 'xx', 'keepttl', 'get')

    def __init__(self):
        self.key: str
        self.value: str
        self.ex: Optional[int] = None  # Expire time in seconds
        self.px: Optional[int] = None  # Expire time in milliseconds
        self.exat: Optional[int] = None  # Expire timestamp in seconds
        self.pxat: Optional[int] = None  # Expire timestamp in milliseconds
        # Existence options
        self.nx: bool = False  # Only set if key does not exist
        self.xx: bool = False  # Only set if key exists
        # Other options
        self.keepttl: bool = False  # Retain the TTL associated with the key
        self.get: bool = False  # Return the old value stored at key

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('set command requires key and value')

        self.key = tokens[1]
        self.value = tokens[2]

        i = 3
        while i < len(tokens):
            opt = tokens[i].upper()

            # Handle options without values
            if opt in ['NX', 'XX', 'KEEPTTL', 'GET']:
                if opt == 'NX':
                    if self.xx:
                        raise ValueError('NX and XX options are mutually exclusive')
                    self.nx = True
                elif opt == 'XX':
                    if self.nx:
                        raise ValueError('NX and XX options are mutually exclusive')
                    self.xx = True
                elif opt == 'KEEPTTL':
                    self.keepttl = True
                elif opt == 'GET':
                    self.get = True
                i += 1
                continue

            # Handle options with values
            if i + 1 >= len(tokens):
                raise ValueError(f'option {opt.lower()} requires a value')

            try:
                val = int(tokens[i + 1])
                if val <= 0:
                    raise ValueError('expiration time must be positive')

                # Handle expiration options
                if opt == 'EX':
                    self.ex = val
                elif opt == 'PX':
                    self.px = val
                elif opt == 'EXAT':
                    self.exat = val
                elif opt == 'PXAT':
                    self.pxat = val
                else:
                    raise ValueError(f'invalid option: {opt.lower()}')

            except ValueError as e:
                if 'invalid literal for int()' in str(e):
                    raise ValueError('invalid expiration time')
                raise

            i += 2

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db

        # Check existence conditions
        key_exists = db.exists(self.key)
        if self.nx and key_exists:
            return None
        if self.xx and not key_exists:
            return None

        # Get old value if requested
        old_value = None
        if self.get:
            if key_exists:
                old_value = db.get_str(self.key)

        # Set the new value
        db.set(self.key, self.value)

        # Handle expiration
        if not self.keepttl:
            ctx.db.delete_expiration(self.key)

        if self.ex is not None:
            ex = (time.time() + self.ex) * 1000
            db.set_expiration(self.key, int(ex))
        elif self.px is not None:
            px = time.time() * 1000 + self.px
            db.set_expiration(self.key, int(px))
        elif self.exat is not None:
            db.set_expiration(self.key, self.exat * 1000)
        elif self.pxat is not None:
            db.set_expiration(self.key, self.pxat)

        return old_value if self.get else 'OK'


class GetCommand(ReadCommand):
    name = 'get'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        """Parse command arguments"""
        if len(tokens) < 2:
            raise ValueError('get command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db

        value = db.get(self.key)
        if value is not None and not isinstance(value, str):
            raise TypeError('value is not a string')

        return value


class AppendCommand(WriteCommand):
    name = 'append'
    __slots__ = ('key', 'value')

    def __init__(self):
        self.key: str
        self.value: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('append command requires key and value')
        self.key = tokens[1]
        self.value = tokens[2]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            db.set(self.key, self.value)
            return len(self.value)

        old_value = db.get_str(self.key)

        new_value = old_value + self.value
        db.set(self.key, new_value)
        return len(new_value)


class DecrbyCommand(WriteCommand):
    name = 'decrby'
    __slots__ = ('key', 'decrement')

    def __init__(self):
        self.key: str
        self.decrement: int

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('decrby command requires key and decrement')
        self.key = tokens[1]
        try:
            self.decrement = int(tokens[2])
        except ValueError:
            raise ValueError('decrement must be an integer')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            value = 0
        else:
            value = db.get_str(self.key)
            try:
                value = int(value)
            except ValueError:
                raise ValueError("value is not an integer")

        new_value = str(value - self.decrement)
        db.set(self.key, new_value)
        return new_value


class DeleteCommand(WriteCommand):
    name = 'del'
    __slots__ = ('keys',)

    def __init__(self):
        self.keys: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('del command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        deleted = 0
        for key in self.keys:
            deleted += ctx.db.delete(key)
        return deleted


class ExistsCommand(ReadCommand):
    name = 'exists'
    __slots__ = ('keys',)

    def __init__(self):
        self.keys: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('exists command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        count = 0
        for key in self.keys:
            if ctx.db.exists(key):
                count += 1
        return count


class CopyCommand(WriteCommand):
    name = 'copy'
    __slots__ = ('source', 'destination', 'replace')

    def __init__(self):
        self.source: str
        self.destination: str
        self.replace: bool

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('copy command requires source and destination')
        self.source = tokens[1]
        self.destination = tokens[2]
        self.replace = False

        if len(tokens) > 3:
            if tokens[3].lower() == 'replace':
                self.replace = True

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

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
    __slots__ = ('key', 'seconds', 'nx', 'xx', 'gt', 'lt')

    def __init__(self):
        self.key: str
        self.seconds: int
        self.nx: bool = False
        self.xx: bool = False
        self.gt: bool = False
        self.lt: bool = False

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('expire command requires key and seconds')
        self.key = tokens[1]
        try:
            self.seconds = int(tokens[2])
        except ValueError:
            raise ValueError('seconds must be an integer')
        if self.seconds < 0:
            raise ValueError('seconds must be >= 0')

        # Parse options
        i = 3
        while i < len(tokens):
            opt = tokens[i].upper()
            if opt == 'NX':
                if self.xx:
                    raise ValueError('NX and XX options are mutually exclusive')
                self.nx = True
            elif opt == 'XX':
                if self.nx:
                    raise ValueError('NX and XX options are mutually exclusive')
                self.xx = True
            elif opt == 'GT':
                if self.lt:
                    raise ValueError('GT and LT options are mutually exclusive')
                self.gt = True
            elif opt == 'LT':
                if self.gt:
                    raise ValueError('GT and LT options are mutually exclusive')
                self.lt = True
            else:
                raise ValueError(f'invalid option: {opt}')
            i += 1

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        now = int(time.time() * 1000)
        new_expiration = now + self.seconds * 1000
        current_expiration = db.get_expiration(self.key)

        # Check NX/XX conditions
        if self.nx and current_expiration != -1:
            return 0
        if self.xx and current_expiration == -1:
            return 0

        # Check GT/LT conditions
        if current_expiration != -1:
            if self.gt and new_expiration <= current_expiration:
                return 0
            if self.lt and new_expiration >= current_expiration:
                return 0

        return db.set_expiration(self.key, new_expiration)


class ExpireatCommand(WriteCommand):
    name = 'expireat'
    __slots__ = ('key', 'timestamp', 'nx', 'xx', 'gt', 'lt')

    def __init__(self):
        self.key: str
        self.timestamp: int
        self.nx: bool = False
        self.xx: bool = False
        self.gt: bool = False
        self.lt: bool = False

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('expireat command requires key and timestamp')
        self.key = tokens[1]
        try:
            self.timestamp = int(tokens[2])
        except ValueError:
            raise ValueError('timestamp must be an integer')

        # Parse options
        i = 3
        while i < len(tokens):
            opt = tokens[i].upper()
            if opt == 'NX':
                if self.xx:
                    raise ValueError('NX and XX options are mutually exclusive')
                self.nx = True
            elif opt == 'XX':
                if self.nx:
                    raise ValueError('NX and XX options are mutually exclusive')
                self.xx = True
            elif opt == 'GT':
                if self.lt:
                    raise ValueError('GT and LT options are mutually exclusive')
                self.gt = True
            elif opt == 'LT':
                if self.gt:
                    raise ValueError('GT and LT options are mutually exclusive')
                self.lt = True
            else:
                raise ValueError(f'invalid option: {opt}')
            i += 1

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        # Convert seconds to milliseconds
        new_expiration = self.timestamp * 1000
        current_expiration = db.get_expiration(self.key)

        # Check NX/XX conditions
        if self.nx and current_expiration != -1:
            return 0
        if self.xx and current_expiration == -1:
            return 0

        # Check GT/LT conditions
        if current_expiration != -1:
            if self.gt and new_expiration <= current_expiration:
                return 0
            if self.lt and new_expiration >= current_expiration:
                return 0

        return db.set_expiration(self.key, new_expiration)


class ExpireTimeCommand(ReadCommand):
    name = 'expiretime'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('expiretime command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db

        if not db.exists(self.key):  # Key does not exist
            return -2
        expiration = db.get_expiration(self.key)
        if expiration == -1:  # Key exists but has no expiration
            return -1
        # Convert milliseconds to seconds
        return expiration // 1000


class IncrbyCommand(WriteCommand):
    name = 'incrby'
    __slots__ = ('key', 'increment')

    def __init__(self):
        self.key: str
        self.increment: int

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('incrby command requires key and increment')
        self.key = tokens[1]
        try:
            self.increment = int(tokens[2])
        except ValueError:
            raise ValueError('increment must be an integer')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            value = 0
        else:
            value = db.get_str(self.key)
            try:
                value = int(value)
            except ValueError:
                raise ValueError("value is not an integer")

        new_value = str(value + self.increment)
        db.set(self.key, new_value)
        return new_value


class IncrbyfloatCommand(WriteCommand):
    name = 'incrbyfloat'
    __slots__ = ('key', 'increment')

    def __init__(self):
        self.key: str
        self.increment: float

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('incrbyfloat command requires key and increment')
        self.key = tokens[1]
        try:
            self.increment = float(tokens[2])
        except ValueError:
            raise ValueError('increment must be a float')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            value = 0.0
        else:
            value = db.get_str(self.key)
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
    __slots__ = ('pattern',)

    def __init__(self):
        self.pattern: str

    def _parse(self, tokens: List[str]):
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
        i = 0
        result = []
        while i < len(pattern):
            if pattern[i] == '\\' and i + 1 < len(pattern):
                # Handle escaped characters - make them match literally
                next_char = pattern[i + 1]
                if next_char in '*?[]\\':
                    result.append(re.escape(next_char))
                else:
                    # If not escaping a special char, keep the escape sequence
                    result.extend(['\\', next_char])
                i += 2
            elif pattern[i] == '*':
                result.append('.*')
                i += 1
            elif pattern[i] == '?':
                result.append('.')
                i += 1
            elif pattern[i] == '[':
                # Handle character classes [...]
                bracket_content = []
                i += 1
                while i < len(pattern) and pattern[i] != ']':
                    if pattern[i] == '\\' and i + 1 < len(pattern):
                        bracket_content.append(re.escape(pattern[i + 1]))
                        i += 2
                    else:
                        bracket_content.append(pattern[i])
                        i += 1
                if i < len(pattern) and pattern[i] == ']':
                    result.append('[' + ''.join(bracket_content) + ']')
                    i += 1
                else:
                    # If no matching ], treat [ as literal
                    result.append('\\[')
                    i = pattern.find('[') + 1
            else:
                # Escape other regex special chars
                result.append(re.escape(pattern[i]))
                i += 1

        # Add anchors to match entire string
        return f'^{"".join(result)}$'

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

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


class MGetCommand(ReadCommand):
    name = 'mget'
    __slots__ = ('keys',)

    def __init__(self):
        self.keys: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('mget command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        return [db.get(key) for key in self.keys]


class MSetCommand(WriteCommand):
    name = 'mset'
    __slots__ = ('pairs',)

    def __init__(self):
        self.pairs: List[Tuple[str, str]]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3 or len(tokens) % 2 != 1:
            raise ValueError('mset command requires key value pairs')

        # Convert flat list to pairs
        self.pairs = []
        for i in range(1, len(tokens), 2):
            self.pairs.append((tokens[i], tokens[i + 1]))

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        for key, value in self.pairs:
            db.set(key, value)
        return "OK"


class MSetnxCommand(WriteCommand):
    name = 'msetnx'
    __slots__ = ('pairs',)

    def __init__(self):
        self.pairs: List[Tuple[str, str]]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3 or len(tokens) % 2 != 1:
            raise ValueError('msetnx command requires key value pairs')

        # Convert flat list to pairs
        self.pairs = []
        for i in range(1, len(tokens), 2):
            self.pairs.append((tokens[i], tokens[i + 1]))

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

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
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('persist command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

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
    __slots__ = ()

    def _parse(self, tokens: List[str]):
        if len(tokens) > 1:
            raise ValueError('randomkey command takes no arguments')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        keys = list(ctx.db.keys())
        if not keys:
            return None
        return random.choice(keys)


class RenameCommand(WriteCommand):
    name = 'rename'
    __slots__ = ('source', 'destination')

    def __init__(self):
        self.source: str
        self.destination: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError(f'{self.name} command requires source and destination')
        self.source = tokens[1]
        self.destination = tokens[2]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)
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
    __slots__ = ()

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        if ctx.db.exists(self.destination):
            return 0

        super().execute(ctx)

        return 1


class StrlenCommand(ReadCommand):
    name = 'strlen'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('strlen command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_str(self.key)

        return len(value)


class SubstrCommand(ReadCommand):
    name = 'substr'
    __slots__ = ('key', 'start', 'end')

    def __init__(self):
        self.key: str
        self.start: int
        self.end: int

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('substr command requires key, start and end')
        self.key = tokens[1]
        try:
            self.start = int(tokens[2])
            self.end = int(tokens[3])
        except ValueError:
            raise ValueError('start and end must be integers')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return None

        value = db.get_str(self.key)

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
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('ttl command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db

        if not db.exists(self.key):
            return -2  # Key does not exist
        expiration = db.get_expiration(self.key)
        if expiration == -1:
            return -1  # Key exists but has no associated expire

        now = int(time.time() * 1000)
        remaining = expiration - now

        # Return remaining time in seconds, rounded down
        return max(remaining // 1000, 0)


class PTTLCommand(ReadCommand):
    name = 'pttl'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('pttl command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db

        if not db.exists(self.key):
            return -2  # Key does not exist
        expiration = db.get_expiration(self.key)
        if expiration == -1:
            return -1  # Key exists but has no associated expire

        now = int(time.time() * 1000)
        remaining = expiration - now

        return max(remaining, 0)


class TypeCommand(ReadCommand):
    name = 'type'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('type command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return "none"

        return db.get_type(self.key)
