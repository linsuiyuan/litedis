import random
from typing import List, Optional

from litedis.core.command.base import CommandContext, ReadCommand, WriteCommand


class SAddCommand(WriteCommand):
    name = 'sadd'
    __slots__ = ('key', 'members')

    def __init__(self):
        self.key: str
        self.members: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('sadd command requires key and at least one member')
        self.key = tokens[1]
        self.members = tokens[2:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            value = set()
        else:
            value = db.get_set(self.key)

        # Count new members added
        added = 0
        for member in self.members:
            if member not in value:
                value.add(member)
                added += 1

        db.set(self.key, value)
        return added


class SCardCommand(ReadCommand):
    name = 'scard'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('scard command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_set(self.key)

        return len(value)


class SDiffCommand(ReadCommand):
    name = 'sdiff'
    __slots__ = ('keys',)

    def __init__(self):
        self.keys: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('sdiff command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        result = None

        # Process each key
        for i, key in enumerate(self.keys):
            if not db.exists(key):
                if i == 0:  # If first key doesn't exist
                    return []
                continue  # Skip non-existent keys after first

            value = db.get_set(key)

            if i == 0:  # First set
                result = value.copy()
            else:  # Subtract subsequent sets
                result -= value

        return list(result) if result else []


class SInterCommand(ReadCommand):
    """Intersect multiple sets"""
    name = 'sinter'
    __slots__ = ('keys',)

    def __init__(self):
        self.keys: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('sinter command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        result = None
        # Process each key
        for key in self.keys:
            if not db.exists(key):
                return []  # If any key doesn't exist, result is empty

            value = db.get_set(key)

            if result is None:  # First set
                result = value.copy()
            else:  # Intersect with subsequent sets
                result &= value

            if not result:  # Optimization: stop if intersection becomes empty
                break

        return list(result) if result else []


class SInterCardCommand(ReadCommand):
    name = 'sintercard'
    __slots__ = ('numkeys', 'keys', 'limit')

    def __init__(self):
        self.numkeys: int
        self.keys: List[str]
        self.limit: Optional[int]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('sintercard command requires numkeys and at least one key')

        try:
            self.numkeys = int(tokens[1])
        except ValueError:
            raise ValueError('numkeys must be a positive integer')
        if self.numkeys < 1:
            raise ValueError('numkeys must be positive')

        if len(tokens) < 2 + self.numkeys:
            raise ValueError('number of keys does not match numkeys')

        self.keys = tokens[2:2 + self.numkeys]
        self.limit = None

        # Parse optional LIMIT argument
        i = 2 + self.numkeys
        if i + 1 < len(tokens):
            if tokens[i].upper() != 'LIMIT':
                raise ValueError('invalid argument')
            try:
                self.limit = int(tokens[i + 1])
            except ValueError:
                raise ValueError('limit must be a non-negative integer')
            if self.limit < 0:
                raise ValueError('limit must be non-negative')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        result = None

        # Process each key
        for key in self.keys:
            if not db.exists(key):
                return 0  # If any key doesn't exist, cardinality is 0

            value = db.get_set(key)

            if result is None:  # First set
                result = value.copy()
            else:  # Intersect with subsequent sets
                result &= value

                # Early return if limit is reached
                if self.limit is not None and len(result) <= self.limit:
                    break

        cardinality = len(result) if result else 0
        if self.limit is not None:
            cardinality = min(cardinality, self.limit)

        return cardinality


class SIsMemberCommand(ReadCommand):
    name = 'sismember'
    __slots__ = ('key', 'member')

    def __init__(self):
        self.key: str
        self.member: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('sismember command requires key and member')
        self.key = tokens[1]
        self.member = tokens[2]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_set(self.key)

        return 1 if self.member in value else 0


class SMembersCommand(ReadCommand):
    name = 'smembers'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('smembers command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get_set(self.key)

        return list(value)


class SMIsMemberCommand(ReadCommand):
    name = 'smismember'
    __slots__ = ('key', 'members')

    def __init__(self):
        self.key: str
        self.members: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('smismember command requires key and at least one member')
        self.key = tokens[1]
        self.members = tokens[2:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return [0] * len(self.members)

        value = db.get_set(self.key)

        return [1 if member in value else 0 for member in self.members]


class SMoveCommand(WriteCommand):
    name = 'smove'
    __slots__ = ('source', 'destination', 'member')

    def __init__(self):
        self.source: str
        self.destination: str
        self.member: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('smove command requires source, destination and member')
        self.source = tokens[1]
        self.destination = tokens[2]
        self.member = tokens[3]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db

        # Check source set
        if not db.exists(self.source):
            return 0

        try:
            source_set = db.get_set(self.source)
        except TypeError:
            raise TypeError('source value is not a set')

        # Check if member exists in source
        if self.member not in source_set:
            return 0

        # Get or create destination set
        if not db.exists(self.destination):
            dest_set = set()
        else:
            try:
                dest_set = db.get_set(self.destination)
            except TypeError:
                raise TypeError('destination value is not a set')

        # Move member
        source_set.remove(self.member)
        dest_set.add(self.member)

        # Update both sets
        if source_set:
            db.set(self.source, source_set)
        else:
            db.delete(self.source)

        db.set(self.destination, dest_set)
        return 1


class SPopCommand(WriteCommand):
    name = 'spop'
    __slots__ = ('key', 'count')

    def __init__(self):
        self.key: str
        self.count: Optional[int]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('spop command requires key')
        self.key = tokens[1]
        self.count = None

        if len(tokens) > 2:
            try:
                self.count = int(tokens[2])
            except ValueError:
                raise ValueError('count must be a positive integer')
            if self.count < 0:
                raise ValueError('count must be positive')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return None if self.count is None else []

        value = db.get_set(self.key)

        if not value:
            return None if self.count is None else []

        if self.count is None:
            # Pop single element
            result = random.choice(list(value))
            value.remove(result)
        else:
            # Pop multiple elements
            count = min(self.count, len(value))
            result = random.sample(list(value), count)
            value -= set(result)

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return result


class SRandMemberCommand(ReadCommand):
    name = 'srandmember'
    __slots__ = ('key', 'count')

    def __init__(self):
        self.key: str
        self.count: Optional[int]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('srandmember command requires key')
        self.key = tokens[1]
        self.count = None

        if len(tokens) > 2:
            try:
                self.count = int(tokens[2])
            except ValueError:
                raise ValueError('count must be an integer')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return None if self.count is None else []

        value = db.get_set(self.key)

        if not value:
            return None if self.count is None else []

        value_list = list(value)
        if self.count is None:
            # Return single element
            return random.choice(value_list)

        if self.count >= 0:
            # Return count distinct elements
            count = min(self.count, len(value))
            return random.sample(value_list, count)
        else:
            # Return |count| elements with possible repeats
            return [random.choice(value_list) for _ in range(-self.count)]


class SRemCommand(WriteCommand):
    name = 'srem'
    __slots__ = ('key', 'members')

    def __init__(self):
        self.key: str
        self.members: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('srem command requires key and at least one member')
        self.key = tokens[1]
        self.members = tokens[2:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_set(self.key)

        removed = 0
        for member in self.members:
            if member in value:
                value.remove(member)
                removed += 1

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return removed


class SUnionCommand(ReadCommand):
    name = 'sunion'
    __slots__ = ('keys',)

    def __init__(self):
        self.keys: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('sunion command requires at least one key')
        self.keys = tokens[1:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        result = set()

        for key in self.keys:
            if not db.exists(key):
                continue

            value = db.get_set(key)

            result |= value

        return list(result)
