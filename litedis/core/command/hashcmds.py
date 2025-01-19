from typing import List, Optional, Tuple

from litedis.core.command.base import CommandContext, ReadCommand, WriteCommand


class HDelCommand(WriteCommand):
    name = 'hdel'
    __slots__ = ('key', 'fields')

    def __init__(self):
        self.key: str
        self.fields: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('hdel command requires key and at least one field')
        self.key = tokens[1]
        self.fields = tokens[2:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_dict(self.key)

        deleted_count = 0
        for field in self.fields:
            if field in value:
                del value[field]
                deleted_count += 1

        if not value:  # If hash is empty after deletion
            db.delete(self.key)
        else:
            db.set(self.key, value)

        return deleted_count


class HExistsCommand(ReadCommand):
    name = 'hexists'
    __slots__ = ('key', 'field')

    def __init__(self):
        self.key: str
        self.field: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('hexists command requires key and field')
        self.key = tokens[1]
        self.field = tokens[2]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_dict(self.key)

        return 1 if self.field in value else 0


class HGetCommand(ReadCommand):
    name = 'hget'
    __slots__ = ('key', 'field')

    def __init__(self):
        self.key: str
        self.field: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('hget command requires key and field')
        self.key = tokens[1]
        self.field = tokens[2]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return None

        value = db.get_dict(self.key)

        return value.get(self.field)


class HGetAllCommand(ReadCommand):
    name = 'hgetall'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('hgetall command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get_dict(self.key)

        # Return as flat list alternating between field and value
        result = []
        for field, val in value.items():
            result.extend([field, val])
        return result


class HIncrByCommand(WriteCommand):
    name = 'hincrby'
    __slots__ = ('key', 'field', 'increment')

    def __init__(self):
        self.key: str
        self.field: str
        self.increment: int

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('hincrby command requires key, field and increment')
        self.key = tokens[1]
        self.field = tokens[2]
        try:
            self.increment = int(tokens[3])
        except ValueError:
            raise ValueError('increment must be an integer')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            value = {}
        else:
            value = db.get_dict(self.key)

        # Get current field value or initialize to 0
        try:
            current = int(value.get(self.field, "0"))
        except ValueError:
            raise ValueError("value is not an integer")

        # Perform increment
        new_value = current + self.increment
        value[self.field] = str(new_value)
        db.set(self.key, value)

        return new_value


class HIncrByFloatCommand(WriteCommand):
    name = 'hincrbyfloat'
    __slots__ = ('key', 'field', 'increment')

    def __init__(self):
        self.key: str
        self.field: str
        self.increment: float

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('hincrbyfloat command requires key, field and increment')
        self.key = tokens[1]
        self.field = tokens[2]
        try:
            self.increment = float(tokens[3])
        except ValueError:
            raise ValueError('increment must be a float')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            value = {}
        else:
            value = db.get_dict(self.key)

        # Get current field value or initialize to 0
        try:
            current = float(value.get(self.field, "0"))
        except ValueError:
            raise ValueError("value is not a float")

        # Perform increment
        new_value = current + self.increment
        # Remove trailing zeros and decimal point if it's a whole number
        str_value = str(new_value)
        if '.' in str_value:
            str_value = str_value.rstrip('0').rstrip('.')

        value[self.field] = str_value
        db.set(self.key, value)

        return str_value


class HKeysCommand(ReadCommand):
    name = 'hkeys'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('hkeys command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get_dict(self.key)

        return list(value.keys())


class HLenCommand(ReadCommand):
    name = 'hlen'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('hlen command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_dict(self.key)

        return len(value)


class HSetCommand(WriteCommand):
    name = 'hset'
    __slots__ = ('key', 'pairs')

    def __init__(self):
        self.key: str
        self.pairs: List[Tuple[str, str]]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4 or len(tokens) % 2 != 0:
            raise ValueError('hset command requires key and field value pairs')
        self.key = tokens[1]
        # Convert flat list to pairs
        self.pairs = []
        for i in range(2, len(tokens), 2):
            self.pairs.append((tokens[i], tokens[i + 1]))

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            value = {}
        else:
            value = db.get_dict(self.key)

        new_fields = 0
        for field, val in self.pairs:
            if field not in value:
                new_fields += 1
            value[field] = val

        db.set(self.key, value)
        return new_fields


class HSetNXCommand(WriteCommand):
    name = 'hsetnx'
    __slots__ = ('key', 'field', 'value')

    def __init__(self):
        self.key: str
        self.field: str
        self.value: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('hsetnx command requires key, field and value')
        self.key = tokens[1]
        self.field = tokens[2]
        self.value = tokens[3]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            value = {}
        else:
            value = db.get_dict(self.key)

            # If field already exists, return 0
            if self.field in value:
                return 0

        value[self.field] = self.value
        db.set(self.key, value)
        return 1


class HMGetCommand(ReadCommand):
    """Get the values of all the given hash fields"""
    name = 'hmget'
    __slots__ = ('key', 'fields')

    def __init__(self):
        self.key: str
        self.fields: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('hmget command requires key and at least one field')
        self.key = tokens[1]
        self.fields = tokens[2:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return [None] * len(self.fields)

        value = db.get_dict(self.key)

        # Return None for non-existing fields
        return [value.get(field) for field in self.fields]


class HValsCommand(ReadCommand):
    name = 'hvals'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('hvals command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get_dict(self.key)

        return list(value.values())


class HStrLenCommand(ReadCommand):
    name = 'hstrlen'
    __slots__ = ('key', 'field')

    def __init__(self):
        self.key: str
        self.field: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('hstrlen command requires key and field')
        self.key = tokens[1]
        self.field = tokens[2]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_dict(self.key)

        field_value = value.get(self.field)
        if field_value is None:
            return 0

        return len(str(field_value))


class HScanCommand(ReadCommand):
    name = 'hscan'
    __slots__ = ('key', 'cursor', 'pattern', 'count')

    def __init__(self):
        self.key: str
        self.cursor: int
        self.pattern: Optional[str]
        self.count: int

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('hscan command requires key and cursor')
        self.key = tokens[1]
        try:
            self.cursor = int(tokens[2])
        except ValueError:
            raise ValueError('cursor must be an integer')

        self.pattern = None
        self.count = 10  # Default count

        # Parse optional arguments
        i = 3
        while i < len(tokens):
            if tokens[i].lower() == 'match' and i + 1 < len(tokens):
                self.pattern = tokens[i + 1]
                i += 2
            elif tokens[i].lower() == 'count' and i + 1 < len(tokens):
                try:
                    self.count = int(tokens[i + 1])
                except ValueError:
                    raise ValueError('count must be an integer')
                i += 2
            else:
                raise ValueError('invalid argument')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return [0, []]

        value = db.get_dict(self.key)

        # Convert items to list and filter by pattern
        items = []
        for field, val in value.items():
            if self.pattern is None or self._matches_pattern(field, self.pattern):
                items.extend([field, val])

        # If cursor is 0 or beyond list length, start from beginning
        if self.cursor >= len(items) or self.cursor == 0:
            start_index = 0
        else:
            start_index = self.cursor

        # Calculate end index based on count
        end_index = min(start_index + (self.count * 2), len(items))

        # Get the subset of items for this iteration
        result_items = items[start_index:end_index]

        # Calculate next cursor
        next_cursor = end_index if end_index < len(items) else 0

        return [next_cursor, result_items]

    def _matches_pattern(self, s: str, pattern: str) -> bool:
        """Simple pattern matching supporting only * wildcard"""
        import fnmatch
        return fnmatch.fnmatch(s, pattern)
