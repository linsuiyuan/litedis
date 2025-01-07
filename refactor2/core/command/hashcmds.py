from refactor2.core.command.base import CommandContext, ReadCommand, WriteCommand


class HDelCommand(WriteCommand):
    name = 'hdel'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.fields: list[str]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('hdel command requires key and at least one field')
        self.key = tokens[1]
        self.fields = tokens[2:]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

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

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.field: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('hexists command requires key and field')
        self.key = tokens[1]
        self.field = tokens[2]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

        return 1 if self.field in value else 0


class HGetCommand(ReadCommand):
    name = 'hget'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.field: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('hget command requires key and field')
        self.key = tokens[1]
        self.field = tokens[2]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return None

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

        return value.get(self.field)


class HGetAllCommand(ReadCommand):
    name = 'hgetall'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('hgetall command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

        # Return as flat list alternating between field and value
        result = []
        for field, val in value.items():
            result.extend([field, val])
        return result


class HIncrByCommand(WriteCommand):
    name = 'hincrby'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.field: str
        self.increment: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 4:
            raise ValueError('hincrby command requires key, field and increment')
        self.key = tokens[1]
        self.field = tokens[2]
        try:
            self.increment = int(tokens[3])
        except ValueError:
            raise ValueError('increment must be an integer')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            value = {}
        else:
            value = db.get(self.key)
            if not isinstance(value, dict):
                raise TypeError("value is not a hash")

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

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.field: str
        self.increment: float
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 4:
            raise ValueError('hincrbyfloat command requires key, field and increment')
        self.key = tokens[1]
        self.field = tokens[2]
        try:
            self.increment = float(tokens[3])
        except ValueError:
            raise ValueError('increment must be a float')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            value = {}
        else:
            value = db.get(self.key)
            if not isinstance(value, dict):
                raise TypeError("value is not a hash")

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

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('hkeys command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

        return list(value.keys())


class HLenCommand(ReadCommand):
    name = 'hlen'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('hlen command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

        return len(value)


class HSetCommand(WriteCommand):
    name = 'hset'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.pairs: list[tuple[str, str]]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 4 or len(tokens) % 2 != 0:
            raise ValueError('hset command requires key and field value pairs')
        self.key = tokens[1]
        # Convert flat list to pairs
        self.pairs = []
        for i in range(2, len(tokens), 2):
            self.pairs.append((tokens[i], tokens[i + 1]))

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            value = {}
        else:
            value = db.get(self.key)
            if not isinstance(value, dict):
                raise TypeError("value is not a hash")

        new_fields = 0
        for field, val in self.pairs:
            if field not in value:
                new_fields += 1
            value[field] = val

        db.set(self.key, value)
        return new_fields


class HSetNXCommand(WriteCommand):
    name = 'hsetnx'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.field: str
        self.value: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 4:
            raise ValueError('hsetnx command requires key, field and value')
        self.key = tokens[1]
        self.field = tokens[2]
        self.value = tokens[3]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            value = {}
        else:
            value = db.get(self.key)
            if not isinstance(value, dict):
                raise TypeError("value is not a hash")

            # If field already exists, return 0
            if self.field in value:
                return 0

        value[self.field] = self.value
        db.set(self.key, value)
        return 1


class HMGetCommand(ReadCommand):
    """Get the values of all the given hash fields"""
    name = 'hmget'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.fields: list[str]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('hmget command requires key and at least one field')
        self.key = tokens[1]
        self.fields = tokens[2:]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return [None] * len(self.fields)

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

        # Return None for non-existing fields
        return [value.get(field) for field in self.fields]


class HValsCommand(ReadCommand):
    name = 'hvals'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('hvals command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

        return list(value.values())


class HStrLenCommand(ReadCommand):
    name = 'hstrlen'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.field: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('hstrlen command requires key and field')
        self.key = tokens[1]
        self.field = tokens[2]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

        field_value = value.get(self.field)
        if field_value is None:
            return 0

        return len(str(field_value))


class HScanCommand(ReadCommand):
    name = 'hscan'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.cursor: int
        self.pattern: str | None
        self.count: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
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
        db = ctx.db
        if not db.exists(self.key):
            return [0, []]

        value = db.get(self.key)
        if not isinstance(value, dict):
            raise TypeError("value is not a hash")

        # Convert items to flat list
        items = []
        for field, val in value.items():
            # Apply pattern matching if pattern is specified
            if self.pattern is None or self._matches_pattern(field, self.pattern):
                items.extend([field, val])

        # Simple implementation: return all items at once
        # In a real Redis implementation, this would be paginated
        return [0, items]

    def _matches_pattern(self, s: str, pattern: str) -> bool:
        """Simple pattern matching supporting only * wildcard"""
        import fnmatch
        return fnmatch.fnmatch(s, pattern)
