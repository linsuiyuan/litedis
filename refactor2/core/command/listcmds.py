from refactor2.core.command.base import CommandContext, ReadCommand, WriteCommand


class LIndexCommand(ReadCommand):
    name = 'lindex'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.index: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError('lindex command requires key and index')
        self.key = tokens[1]
        try:
            self.index = int(tokens[2])
        except ValueError:
            raise ValueError('index must be an integer')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return None

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        # Handle negative indices
        index = self.index
        if index < 0:
            index = len(value) + index

        # Check bounds
        if index < 0 or index >= len(value):
            return None

        return value[index]


class LInsertCommand(WriteCommand):
    name = 'linsert'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.before: bool
        self.pivot: str
        self.element: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 5:
            raise ValueError('linsert command requires key, BEFORE|AFTER, pivot and element')
        self.key = tokens[1]

        position = tokens[2].upper()
        if position not in ['BEFORE', 'AFTER']:
            raise ValueError('second argument must be BEFORE or AFTER')
        self.before = position == 'BEFORE'

        self.pivot = tokens[3]
        self.element = tokens[4]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        # Find pivot
        try:
            pivot_index = value.index(self.pivot)
        except ValueError:
            return -1  # Pivot not found

        # Insert element
        insert_index = pivot_index if self.before else pivot_index + 1
        value.insert(insert_index, self.element)
        db.set(self.key, value)

        return len(value)


class LLenCommand(ReadCommand):
    name = 'llen'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('llen command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        return len(value)


class LPopCommand(WriteCommand):
    name = 'lpop'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.count: int | None
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('lpop command requires key')
        self.key = tokens[1]
        self.count = None

        if len(tokens) > 2:
            try:
                self.count = int(tokens[2])
                if self.count < 0:
                    raise ValueError('count must be positive')
            except ValueError:
                raise ValueError('count must be a positive integer')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return None

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        if not value:
            return None

        if self.count is None:
            # Pop single element
            result = value.pop(0)
        else:
            # Pop multiple elements
            count = min(self.count, len(value))
            result = value[:count]
            value = value[count:]

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return result


class LPushCommand(WriteCommand):
    name = 'lpush'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.elements: list[str]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError(f'{self.name} command requires key and at least one element')
        self.key = tokens[1]
        self.elements = tokens[2:]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            value = []
        else:
            value = db.get(self.key)
            if not isinstance(value, list):
                raise TypeError("value is not a list")

        # Prepend elements in reverse order
        for element in reversed(self.elements):
            value.insert(0, element)

        db.set(self.key, value)
        return len(value)


class LPushXCommand(LPushCommand):
    name = 'lpushx'

    def execute(self, ctx: CommandContext):
        if not ctx.db.exists(self.key):
            return 0

        return super().execute(ctx)


class LRangeCommand(ReadCommand):
    name = 'lrange'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.start: int
        self.stop: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 4:
            raise ValueError('lrange command requires key, start and stop')
        self.key = tokens[1]
        try:
            self.start = int(tokens[2])
            self.stop = int(tokens[3])
        except ValueError:
            raise ValueError('start and stop must be integers')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        # Handle negative indices
        start, stop = self.start, self.stop
        length = len(value)
        if start < 0:
            start = length + start
        if stop < 0:
            stop = length + stop

        # Ensure indices are within bounds
        start = max(0, min(start, length))
        stop = max(0, min(stop + 1, length))  # +1 because Redis includes stop index

        return value[start:stop]


class LRemCommand(WriteCommand):
    name = 'lrem'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.count: int
        self.element: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 4:
            raise ValueError('lrem command requires key, count and element')
        self.key = tokens[1]
        try:
            self.count = int(tokens[2])
        except ValueError:
            raise ValueError('count must be an integer')
        self.element = tokens[3]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        removed = 0
        if self.count > 0:
            # Remove count elements from head to tail
            i = 0
            while i < len(value) and removed < self.count:
                if value[i] == self.element:
                    value.pop(i)
                    removed += 1
                else:
                    i += 1
        elif self.count < 0:
            # Remove count elements from tail to head
            i = len(value) - 1
            while i >= 0 and removed < -self.count:
                if value[i] == self.element:
                    value.pop(i)
                    removed += 1
                i -= 1
        else:
            # Remove all elements equal to element
            original_length = len(value)
            value = [x for x in value if x != self.element]
            removed = original_length - len(value)

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return removed


class LSetCommand(WriteCommand):
    name = 'lset'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.index: int
        self.element: str
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 4:
            raise ValueError('lset command requires key, index and element')
        self.key = tokens[1]
        try:
            self.index = int(tokens[2])
        except ValueError:
            raise ValueError('index must be an integer')
        self.element = tokens[3]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            raise ValueError("no such key")

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        # Handle negative indices
        index = self.index
        if index < 0:
            index = len(value) + index

        # Check bounds
        if index < 0 or index >= len(value):
            raise ValueError("index out of range")

        value[index] = self.element
        db.set(self.key, value)
        return "OK"


class LTrimCommand(WriteCommand):
    name = 'ltrim'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.start: int
        self.stop: int
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 4:
            raise ValueError('ltrim command requires key, start and stop')
        self.key = tokens[1]
        try:
            self.start = int(tokens[2])
            self.stop = int(tokens[3])
        except ValueError:
            raise ValueError('start and stop must be integers')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return "OK"

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        # Handle negative indices
        start, stop = self.start, self.stop
        length = len(value)
        if start < 0:
            start = length + start
        if stop < 0:
            stop = length + stop

        # Ensure indices are within bounds
        start = max(0, min(start, length))
        stop = max(0, min(stop + 1, length))  # +1 because Redis includes stop index

        # Trim the list
        value = value[start:stop]

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return "OK"


class RPopCommand(WriteCommand):
    name = 'rpop'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.count: int | None
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('rpop command requires key')
        self.key = tokens[1]
        self.count = None

        if len(tokens) > 2:
            try:
                self.count = int(tokens[2])
                if self.count < 0:
                    raise ValueError('count must be positive')
            except ValueError:
                raise ValueError('count must be a positive integer')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return None

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        if not value:
            return None

        if self.count is None:
            # Pop single element
            result = value.pop()
        else:
            # Pop multiple elements
            count = min(self.count, len(value))
            result = value[-count:]
            value = value[:-count]

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return result


class RPushCommand(WriteCommand):
    name = 'rpush'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.elements: list[str]
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 3:
            raise ValueError(f'{self.name} command requires key and at least one element')
        self.key = tokens[1]
        self.elements = tokens[2:]

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            value = []
        else:
            value = db.get(self.key)
            if not isinstance(value, list):
                raise TypeError("value is not a list")

        # Append elements
        value.extend(self.elements)

        db.set(self.key, value)
        return len(value)


class RPushXCommand(RPushCommand):
    name = 'rpushx'

    def execute(self, ctx: CommandContext):
        if not ctx.db.exists(self.key):
            return 0

        return super().execute(ctx)


class SortCommand(WriteCommand):
    name = 'sort'

    def __init__(self, command_tokens: list[str]):
        self.key: str
        self.desc: bool
        self.alpha: bool
        self.store_key: str | None
        self._parse(command_tokens)

    def _parse(self, tokens: list[str]):
        if len(tokens) < 2:
            raise ValueError('sort command requires key')
        self.key = tokens[1]
        self.desc = False
        self.alpha = False
        self.store_key = None

        i = 2
        while i < len(tokens):
            arg = tokens[i].upper()
            if arg == 'DESC':
                self.desc = True
                i += 1
            elif arg == 'ALPHA':
                self.alpha = True
                i += 1
            elif arg == 'STORE' and i + 1 < len(tokens):
                self.store_key = tokens[i + 1]
                i += 2
            else:
                raise ValueError(f'Invalid argument: {tokens[i]}')

    def execute(self, ctx: CommandContext):
        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get(self.key)
        if not isinstance(value, list):
            raise TypeError("value is not a list")

        # Make a copy of the list
        sorted_list = value.copy()

        # Sort the list
        try:
            if self.alpha:
                # Sort as strings
                sorted_list.sort(reverse=self.desc)
            else:
                # Try to sort as numbers
                sorted_list.sort(key=float, reverse=self.desc)
        except ValueError:
            raise ValueError("one or more elements can't be converted to number")

        # Store the result if requested
        if self.store_key:
            db.set(self.store_key, sorted_list)
            return len(sorted_list)

        return sorted_list
