import random
from collections import OrderedDict
from typing import Iterable


class SortedSet(Iterable):
    """
    Sorted Set class, used for database sorted set type.
    Uses OrderedDict as the underlying ordered structure.
    """

    def __init__(self, iterable: Iterable = None):
        if iterable is None:
            self._data = OrderedDict()
        else:
            self._data = OrderedDict(iterable)
            self._sort_data()

    def members(self):
        return self._data.keys()

    def scores(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def _sort_data(self):
        sortdata = sorted(self, key=lambda x: (x[1], x[0]))
        self._data = OrderedDict(sortdata)

    def __contains__(self, m) -> bool:
        return m in self._data

    def __iter__(self):
        return iter(self.items())

    def __getitem__(self, item):
        return self._data[item]

    def __setitem__(self, key, value):
        self._data[key] = float(value)

        self._sort_data()

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return f"{list(self)!r}"

    def add(self, item: (str, float)):
        """
        Add an element to the sorted set. If it exists, update it.
        :param item: (member, score) tuple
        """
        m, s = item
        self[m] = s

    def count(self, min_: float, max_: float) -> int:
        """
        Count elements with scores between min_ and max_
        """
        if min_ > max_:
            raise ValueError("min_ cannot be greater than max_")

        c = 0
        for s in self.scores():
            if s < min_:
                continue
            if s > max_:
                break
            c += 1

        return c

    def difference(self, other: "SortedSet"):
        """
        Set difference
        :param other: Another SortedSet
        :return: A new SortedSet containing elements in self but not in other
        """
        ms = self.members() - other.members()
        return SortedSet({m: self[m] for m in ms})

    __sub__ = difference

    def get(self, member, default=None):
        """
        Get the score of a member
        :param member: Member to look up
        :param default: Default value if member not found
        :return: Score of the member or default value
        """
        return self._data.get(member, default)

    def incr(self, member: str, amount: float) -> float:
        """
        Increment the score of a member. If member doesn't exist,
        initialize it with the given score.
        :param member: Member to increment
        :param amount: Amount to increment by
        :return: The new score
        """
        if member in self:
            self[member] += amount
        else:
            self[member] = amount

        return self[member]

    def intersection(self, other: "SortedSet"):
        """
        Set intersection
        :param other: Another SortedSet
        :return: A new SortedSet containing elements present in both sets
        """
        ms = self.members() & other.members()
        return SortedSet({m: self[m] for m in ms})

    __and__ = intersection

    def pop(self, member, default=None):
        """
        Remove and return the score of a member
        :param member: Member to remove
        :param default: Default value if member not found
        :return: Score of the removed member or default value
        """
        return self._data.pop(member, default)

    def popitem(self, last=True):
        """
        Pop an item from either end
        :param last: If True, pop from the end; if False, pop from the beginning
        :return: (member, score) tuple
        """
        return self._data.popitem(last=last)

    def randmember(self, count: int = 1, unique=True):
        """
        Get random members
        :param count: Number of members to return, default 1
        :param unique: Whether returned members should be unique
        :return: List of (member, score) tuples
        """
        if unique:
            return random.sample(list(self), count)
        else:
            return random.choices(list(self), k=count)

    def range(self,
              start: int,
              end: int,
              min_: float | None = None,
              max_: float | None = None,
              desc: bool = False,
              ) -> list:
        """
        Get member-score pairs within index range or score range
        :param start: Start index
        :param end: End index
        :param min_: Minimum score
        :param max_: Maximum score
        :param desc: Sort in descending order if True
        :return: List of (member, score) tuples
        """

        if desc:
            sorted_items = sorted(self, key=lambda x: (x[1], x[0]), reverse=True)
        else:
            sorted_items = list(self)

        # Filter by score range
        if min_ is not None and max_ is not None:
            sorted_items = [(m, s)
                            for m, s in sorted_items
                            if min_ <= s <= max_]

        # Handle index range (Redis includes right boundary)
        if end < 0:
            end = len(sorted_items) + end + 1
        else:
            end += 1

        if start > end:
            return []

        return sorted_items[start:end]

    def rank(self, member: str, desc=False) -> int | None:
        """
        Get the rank of a member
        :param member: Member to look up
        :param desc: If True, rank in descending order
        :return: Rank of the member or None if not found
        """
        if member not in self:
            return None

        if desc:
            return list(reversed(self.members())).index(member)
        else:
            return list(self.members()).index(member)

    score = get

    def union(self, other: "SortedSet"):
        """
        Set union
        :param other: Another SortedSet
        :return: A new SortedSet containing elements from both sets,
                with scores added for common elements
        """
        # add scores
        temp = {**other._data}
        for member in self.members():
            if member in temp:
                temp[member] += self[member]
        return SortedSet({**self._data, **temp})

    __or__ = union
