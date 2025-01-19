import re
from typing import List, Optional, Tuple

from litedis.core.command.base import CommandContext, ReadCommand, WriteCommand
from litedis.core.command.sortedset import SortedSet


class ZAddCommand(WriteCommand):
    name = 'zadd'
    __slots__ = ('key', 'score_members')

    def __init__(self):
        self.key: str
        self.score_members: List[Tuple[float, str]]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('zadd command requires key, score and member')
        self.key = tokens[1]

        # Parse score-member pairs
        if (len(tokens) - 2) % 2 != 0:
            raise ValueError('score and member must come in pairs')

        self.score_members = []
        i = 2
        while i < len(tokens):
            try:
                score = float(tokens[i])
            except ValueError:
                raise ValueError(f'invalid score: {tokens[i]}')
            member = tokens[i + 1]
            self.score_members.append((score, member))
            i += 2

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            zset = SortedSet()
        else:
            zset = db.get_zset(self.key)

        # Add members
        added = 0
        for score, member in self.score_members:
            if member not in zset:
                added += 1
            zset.add((member, score))

        db.set(self.key, zset)
        return added


class ZCardCommand(ReadCommand):
    name = 'zcard'
    __slots__ = ('key',)

    def __init__(self):
        self.key: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('zcard command requires key')
        self.key = tokens[1]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_zset(self.key)

        return len(value)


class ZCountCommand(ReadCommand):
    name = 'zcount'
    __slots__ = ('key', 'min', 'max')

    def __init__(self):
        self.key: str
        self.min: float
        self.max: float

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('zcount command requires key, min and max')
        self.key = tokens[1]
        try:
            self.min = float(tokens[2])
            self.max = float(tokens[3])
        except ValueError:
            raise ValueError('min and max must be valid float numbers')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_zset(self.key)

        return value.count(self.min, self.max)


class ZDiffCommand(ReadCommand):
    name = 'zdiff'
    __slots__ = ('numkeys', 'keys', 'withscores')

    def __init__(self):
        self.numkeys: int
        self.keys: List[str]
        self.withscores: bool = False  # 添加 withscores 属性

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('zdiff command requires numkeys and at least one key')

        try:
            self.numkeys = int(tokens[1])
        except ValueError:
            raise ValueError('numkeys must be a positive integer')
        if self.numkeys < 1:
            raise ValueError('numkeys must be positive')

        if len(tokens) < 2 + self.numkeys:
            raise ValueError('number of keys does not match numkeys')

        self.keys = tokens[2:2 + self.numkeys]

        # 解析可选的 WITHSCORES 参数
        i = 2 + self.numkeys
        if i < len(tokens) and tokens[i].upper() == 'WITHSCORES':
            self.withscores = True

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        result = None

        # Process each key
        for i, key in enumerate(self.keys):
            if not db.exists(key):
                if i == 0:  # If first key doesn't exist
                    return []
                continue

            value = db.get_zset(key)

            if i == 0:  # First set
                result = value
            else:  # Subtract subsequent sets
                result = result - value

        if not result:
            return []

        if self.withscores:
            # Flatten the result into [member1, score1, member2, score2, ...]
            return [item for pair in result for item in pair]
        return [member for member, _ in result]


class ZIncrByCommand(WriteCommand):
    name = 'zincrby'
    __slots__ = ('key', 'increment', 'member')

    def __init__(self):
        self.key: str
        self.increment: float
        self.member: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('zincrby command requires key, increment and member')
        self.key = tokens[1]
        try:
            self.increment = float(tokens[2])
        except ValueError:
            raise ValueError('increment must be a valid float number')
        self.member = tokens[3]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            zset = SortedSet()
        else:
            zset = db.get_zset(self.key)

        new_score = zset.incr(self.member, self.increment)

        db.set(self.key, zset)
        return new_score


class ZInterCommand(ReadCommand):
    name = 'zinter'
    __slots__ = ('numkeys', 'keys', 'withscores')

    def __init__(self):
        self.numkeys: int
        self.keys: List[str]
        self.withscores: bool = False

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('zinter command requires numkeys and at least one key')

        try:
            self.numkeys = int(tokens[1])
        except ValueError:
            raise ValueError('numkeys must be a positive integer')
        if self.numkeys < 1:
            raise ValueError('numkeys must be positive')

        if len(tokens) < 2 + self.numkeys:
            raise ValueError('number of keys does not match numkeys')

        self.keys = tokens[2:2 + self.numkeys]

        i = 2 + self.numkeys
        if i < len(tokens) and tokens[i].upper() == 'WITHSCORES':
            self.withscores = True

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        result = None

        for key in self.keys:
            if not db.exists(key):
                return []  # If any key doesn't exist, result is empty

            value = db.get_zset(key)

            if result is None:  # First set
                result = value
            else:  # Intersect with subsequent sets
                result = result & value

                if not result:  # Optimization: stop if intersection becomes empty
                    break

        if not result:
            return []

        if self.withscores:
            # Flatten the result into [member1, score1, member2, score2, ...]
            return [item for pair in result for item in pair]
        return [member for member, _ in result]


class ZInterCardCommand(ReadCommand):
    """Return the number of elements in the intersection of multiple sorted sets"""
    name = 'zintercard'
    __slots__ = ('numkeys', 'keys', 'limit')

    def __init__(self):
        self.numkeys: int
        self.keys: List[str]
        self.limit: Optional[int]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('zintercard command requires numkeys and at least one key')

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
            if tokens[i].upper() == 'LIMIT':
                try:
                    self.limit = int(tokens[i + 1])
                except ValueError:
                    raise ValueError('limit must be a non-negative integer')
                if self.limit < 0:
                    raise ValueError('limit must be non-negative')
            else:
                raise ValueError('invalid argument')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        result = None

        for key in self.keys:
            if not db.exists(key):
                return 0  # If any key doesn't exist, cardinality is 0

            value = db.get_zset(key)

            if result is None:  # First set
                result = value
            else:  # Intersect with subsequent sets
                result = result & value

                # Early return if limit is reached
                if self.limit is not None and len(result) <= self.limit:
                    break

        cardinality = len(result) if result else 0
        if self.limit is not None:
            cardinality = min(cardinality, self.limit)

        return cardinality


class ZPopMaxCommand(WriteCommand):
    """Remove and return members with the highest scores in a sorted set"""
    name = 'zpopmax'
    __slots__ = ('key', 'count')

    def __init__(self):
        self.key: str
        self.count: int

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('zpopmax command requires key')
        self.key = tokens[1]
        self.count = 1

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
            return []

        value = db.get_zset(self.key)

        if not value:
            return []

        # Get highest scoring members
        result = []
        for _ in range(min(self.count, len(value))):
            member, score = value.popitem(last=True)
            result.append((member, score))

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return result


class ZPopMinCommand(WriteCommand):
    """Remove and return members with the lowest scores in a sorted set"""
    name = 'zpopmin'
    __slots__ = ('key', 'count')

    def __init__(self):
        self.key: str
        self.count: int

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('zpopmin command requires key')
        self.key = tokens[1]
        self.count = 1

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
            return []

        value = db.get_zset(self.key)

        if not value:
            return []

        # Get lowest scoring members
        result = []
        for _ in range(min(self.count, len(value))):
            member, score = value.popitem(last=False)
            result.append((member, score))

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return result


class ZRandMemberCommand(ReadCommand):
    name = 'zrandmember'
    __slots__ = ('key', 'count', 'withscores')

    def __init__(self):
        self.key: str
        self.count: Optional[int]
        self.withscores: bool

    def _parse(self, tokens: List[str]):
        if len(tokens) < 2:
            raise ValueError('zrandmember command requires key')
        self.key = tokens[1]
        self.count = None
        self.withscores = False

        i = 2
        while i < len(tokens):
            if tokens[i].upper() == 'WITHSCORES':
                self.withscores = True
                i += 1
            else:
                try:
                    self.count = int(tokens[i])
                    i += 1
                except ValueError:
                    raise ValueError('count must be an integer')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return None if self.count is None else []

        value = db.get_zset(self.key)

        if not value:
            return None if self.count is None else []

        # Get random members
        if self.count is None:
            result = value.randmember(1)[0]
            if self.withscores:
                return [result[0], result[1]]
            return result[0]
        else:
            unique = self.count >= 0
            count = abs(self.count)
            result = value.randmember(count, unique=unique)
            if self.withscores:
                # Flatten the result into [member1, score1, member2, score2, ...]
                return [item for pair in result for item in pair]
            return [member for member, _ in result]


class ZMPopCommand(WriteCommand):
    """Remove and return members from one or more sorted sets"""
    name = 'zmpop'
    __slots__ = ('numkeys', 'keys', 'where', 'count')

    def __init__(self):
        self.numkeys: int
        self.keys: List[str]
        self.where: str  # MIN or MAX
        self.count: int

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('zmpop command requires numkeys, keys and WHERE')

        try:
            self.numkeys = int(tokens[1])
        except ValueError:
            raise ValueError('numkeys must be a positive integer')
        if self.numkeys < 1:
            raise ValueError('numkeys must be positive')

        if len(tokens) < 2 + self.numkeys:
            raise ValueError('number of keys does not match numkeys')

        self.keys = tokens[2:2 + self.numkeys]

        where = tokens[2 + self.numkeys].upper()
        if where not in ['MIN', 'MAX']:
            raise ValueError('WHERE must be either MIN or MAX')
        self.where = where

        self.count = 1
        if len(tokens) > 3 + self.numkeys:
            if tokens[3 + self.numkeys].upper() == 'COUNT':
                try:
                    self.count = int(tokens[4 + self.numkeys])
                except (IndexError, ValueError):
                    raise ValueError('COUNT requires a positive integer argument')
                if self.count < 0:
                    raise ValueError('count must be positive')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db

        # Find first non-empty sorted set
        target_key = None
        target_set = None
        for key in self.keys:
            if db.exists(key):
                value = db.get_zset(key)
                if value:
                    target_key = key
                    target_set = value
                    break

        if target_set is None:
            return None

        # Pop elements
        result = []
        for _ in range(min(self.count, len(target_set))):
            member, score = target_set.popitem(last=self.where == 'MAX')
            result.append((member, score))

        # Update or delete the set
        if target_set:
            db.set(target_key, target_set)
        else:
            db.delete(target_key)

        return [target_key, result]


class ZRangeCommand(ReadCommand):
    """Return a range of members in a sorted set"""
    name = 'zrange'
    __slots__ = ('key', 'start', 'stop', 'withscores', 'rev')

    def __init__(self):
        self.key: str
        self.start: int
        self.stop: int
        self.withscores: bool
        self.rev: bool

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('zrange command requires key, start and stop')
        self.key = tokens[1]
        try:
            self.start = int(tokens[2])
            self.stop = int(tokens[3])
        except ValueError:
            raise ValueError('start and stop must be integers')

        self.withscores = False
        self.rev = False

        i = 4
        while i < len(tokens):
            arg = tokens[i].upper()
            if arg == 'WITHSCORES':
                self.withscores = True
            elif arg == 'REV':
                self.rev = True
            else:
                raise ValueError(f'Invalid argument: {tokens[i]}')
            i += 1

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get_zset(self.key)

        result = value.range(self.start, self.stop, desc=self.rev)

        if self.withscores:
            # Flatten the result into [member1, score1, member2, score2, ...]
            return [item for pair in result for item in pair]
        return [member for member, _ in result]


class _ZRangeByScoreCommand(ReadCommand):
    """Return a range of members in a sorted set by score"""
    name = '_zrangebyscore'
    __slots__ = ('desc', 'key', 'min', 'max', 'withscores', 'limit')

    def __init__(self, desc):
        self.desc = desc
        self.key: str
        self.min: float
        self.max: float
        self.withscores: bool = False
        self.limit: Optional[Tuple[int, int]] = None

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError(f'{self.name} command requires key, min and max')
        self.key = tokens[1]
        try:
            self.min = float(tokens[2])
            self.max = float(tokens[3])
        except ValueError:
            raise ValueError('min and max must be valid float numbers')
        if self.min > self.max:
            self.min, self.max = self.max, self.min

        i = 4
        while i < len(tokens):
            if tokens[i].upper() == 'WITHSCORES':
                self.withscores = True
                i = i + 1
            elif tokens[i].upper() == 'LIMIT':
                if i + 2 >= len(tokens):
                    raise ValueError('LIMIT requires two arguments')
                try:
                    offset = int(tokens[i + 1])
                    count = int(tokens[i + 2])
                except ValueError:
                    raise ValueError('LIMIT requires two integers')
                if offset < 0 or count < 0:
                    raise ValueError('LIMIT requires two non-negative integers')
                self.limit = (offset, count)
                i = i + 3

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return []

        value = db.get_zset(self.key)

        # Use range with score limits
        result = value.range(0, -1, min_=self.min, max_=self.max, desc=self.desc)

        if self.limit is not None:
            offset, count = self.limit
            result = result[offset:offset + count]

        if self.withscores:
            # Flatten the result into [member1, score1, member2, score2, ...]
            return [item for pair in result for item in pair]
        return [member for member, _ in result]


class ZRangeByScoreCommand(_ZRangeByScoreCommand):
    """Return a range of members in a sorted set by score"""
    name = 'zrangebyscore'

    def __init__(self):
        super().__init__(desc=False)


class ZRevRangeByScoreCommand(_ZRangeByScoreCommand):
    """Return a range of members in a sorted set by score, with scores ordered from high to low"""
    name = 'zrevrangebyscore'

    def __init__(self):
        super().__init__(desc=True)


class _ZRankCommand(ReadCommand):
    name = '_zrank'
    __slots__ = ('desc', 'key', 'member', 'withscores')

    def __init__(self, desc):
        self.desc = desc
        self.key: str
        self.member: str
        self.withscores: bool = False  # 添加 withscores 属性

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError(f'{self.name} command requires key and member')
        self.key = tokens[1]
        self.member = tokens[2]

        # 解析可选的 WITHSCORES 参数
        if len(tokens) > 3 and tokens[3].upper() == 'WITHSCORES':
            self.withscores = True

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return None

        value = db.get_zset(self.key)

        rank = value.rank(self.member, desc=self.desc)
        if rank is None:
            return None

        if self.withscores:
            score = value.score(self.member)
            return [rank, score]
        return rank


class ZRankCommand(_ZRankCommand):
    """Determine the index of a member in a sorted set"""
    name = 'zrank'

    def __init__(self):
        super().__init__(desc=False)


class ZRemCommand(WriteCommand):
    name = 'zrem'
    __slots__ = ('key', 'members')

    def __init__(self):
        self.key: str
        self.members: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('zrem command requires key and at least one member')
        self.key = tokens[1]
        self.members = tokens[2:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_zset(self.key)

        removed = 0
        for member in self.members:
            if member in value:
                value.pop(member)
                removed += 1

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return removed


class ZRemRangeByScoreCommand(WriteCommand):
    """Remove all members in a sorted set within the given scores"""
    name = 'zremrangebyscore'
    __slots__ = ('key', 'min', 'max')

    def __init__(self):
        self.key: str
        self.min: float
        self.max: float

    def _parse(self, tokens: List[str]):
        if len(tokens) < 4:
            raise ValueError('zremrangebyscore command requires key, min and max')
        self.key = tokens[1]
        try:
            self.min = float(tokens[2])
            self.max = float(tokens[3])
        except ValueError:
            raise ValueError('min and max must be valid float numbers')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return 0

        value = db.get_zset(self.key)

        # Get members to remove
        to_remove = [member for member, score in value.items()
                     if self.min <= score <= self.max]

        # Remove members
        for member in to_remove:
            value.pop(member)

        if value:
            db.set(self.key, value)
        else:
            db.delete(self.key)

        return len(to_remove)


class ZRevRankCommand(_ZRankCommand):
    """Determine the index of a member in a sorted set, with scores ordered from high to low"""
    name = 'zrevrank'

    def __init__(self):
        super().__init__(desc=True)


class ZScanCommand(ReadCommand):
    """Incrementally iterate sorted set elements and associated scores"""
    name = 'zscan'
    __slots__ = ('key', 'cursor', 'pattern', 'count')

    def __init__(self):
        self.key: str
        self.cursor: int
        self.pattern: Optional[str]
        self.count: int

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('zscan command requires key and cursor')
        self.key = tokens[1]
        try:
            self.cursor = int(tokens[2])
        except ValueError:
            raise ValueError('cursor must be a non-negative integer')
        if self.cursor < 0:
            raise ValueError('cursor must be non-negative')

        self.pattern = None
        self.count = 10  # Default count

        i = 3
        while i < len(tokens):
            arg = tokens[i].upper()
            if arg == 'MATCH' and i + 1 < len(tokens):
                self.pattern = tokens[i + 1]
                i += 2
            elif arg == 'COUNT' and i + 1 < len(tokens):
                try:
                    self.count = int(tokens[i + 1])
                    if self.count < 1:
                        raise ValueError
                except ValueError:
                    raise ValueError('count must be a positive integer')
                i += 2
            else:
                raise ValueError(f'Invalid argument: {tokens[i]}')

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return [0, []]

        value = db.get_zset(self.key)

        # Convert to list of member-score pairs
        items = list(value.items())
        total_items = len(items)

        if total_items == 0:
            return [0, []]

        # Calculate start position from cursor
        start_pos = self.cursor % total_items

        # Get items starting from cursor position
        result = []
        count = 0
        pos = start_pos

        while count < self.count and len(result) / 2 < total_items:
            member, score = items[pos]
            if self.pattern is None or self._matches_pattern(member, self.pattern):
                result.extend([member, str(score)])
                count += 1
            pos = (pos + 1) % total_items
            if pos == start_pos:  # We've scanned all items
                break

        # Calculate next cursor
        next_cursor = pos if pos != 0 and count == self.count else 0

        return [next_cursor, result]

    def _matches_pattern(self, string: str, pattern: str) -> bool:
        """
        Match a string against a Redis-style pattern.
        Supports * and ? wildcards:
        * matches zero or more characters
        ? matches exactly one character

        Examples:
            h?llo matches hello, hallo but not heello
            h*llo matches hello, hallo, heello
            h[ae]llo matches hello and hallo, but not hillo
            h[^e]llo matches hallo, hbllo but not hello
            h[a-b]llo matches hallo and hbllo

        Args:
            string: The string to match
            pattern: The pattern to match against

        Returns:
            bool: True if the string matches the pattern
        """
        if not pattern:
            return not string

        pattern = re.escape(pattern)

        pattern = pattern.replace(r'\*', '.*')  # *
        pattern = pattern.replace(r'\?', '.')  # ?

        # handle [...] and [^...]
        pattern = re.sub(r'\\\[(.*?)\\\]', r'[\1]', pattern)  # noqa

        try:
            regex = re.compile(f'^{pattern}$')
            return bool(regex.match(string))
        except re.error:
            return False


class ZScoreCommand(ReadCommand):
    """Get the score associated with the given member"""
    name = 'zscore'
    __slots__ = ('key', 'member')

    def __init__(self):
        self.key: str
        self.member: str

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('zscore command requires key and member')
        self.key = tokens[1]
        self.member = tokens[2]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        if not db.exists(self.key):
            return None

        value = db.get_zset(self.key)

        return value.score(self.member)


class ZUnionCommand(ReadCommand):
    """Return the union of multiple sorted sets"""
    name = 'zunion'
    __slots__ = ('numkeys', 'keys', 'withscores')

    def __init__(self):
        self.numkeys: int
        self.keys: List[str]
        self.withscores: bool

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('zunion command requires numkeys and at least one key')

        try:
            self.numkeys = int(tokens[1])
        except ValueError:
            raise ValueError('numkeys must be a positive integer')
        if self.numkeys < 1:
            raise ValueError('numkeys must be positive')

        if len(tokens) < 2 + self.numkeys:
            raise ValueError('number of keys does not match numkeys')

        self.keys = tokens[2:2 + self.numkeys]
        self.withscores = False

        if len(tokens) > 2 + self.numkeys:
            if tokens[2 + self.numkeys].upper() == 'WITHSCORES':
                self.withscores = True

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)

        db = ctx.db
        result = None

        for key in self.keys:
            if not db.exists(key):
                continue

            value = db.get_zset(key)

            if result is None:
                result = value
            else:
                result = result | value

        if result is None:
            return []

        if self.withscores:
            # Return [member1, score1, member2, score2, ...]
            return [item for pair in result for item in pair]
        return [member for member, _ in result]


class ZMScoreCommand(ReadCommand):
    """Get the score associated with multiple members"""
    name = 'zmscore'
    __slots__ = ('key', 'members')

    def __init__(self):
        self.key: str
        self.members: List[str]

    def _parse(self, tokens: List[str]):
        if len(tokens) < 3:
            raise ValueError('zmscore command requires key and at least one member')
        self.key = tokens[1]
        self.members = tokens[2:]

    def execute(self, ctx: CommandContext):
        self._parse(ctx.cmdtokens)
        
        db = ctx.db
        if not db.exists(self.key):
            return [None] * len(self.members)

        value = db.get_zset(self.key)

        return [value.score(member) for member in self.members]
