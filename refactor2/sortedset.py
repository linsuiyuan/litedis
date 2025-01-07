import random
from collections import OrderedDict
from typing import Iterable


# todo 将中文注释改为英文
class SortedSet(Iterable):
    """
    有序集合类，供数据库有序集合类型使用。
    底层使用 OrderedDict 作为有序结构
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
        添加元素到有序集合，不存在则添加，存在则更新
        :param item: 成员+分数 元组
        """
        m, s = item
        self[m] = s

    def count(self, min_: float, max_: float) -> int:
        """
        分数在 min_ 和 max_ 之间的元素数量
        """
        if min_ > max_:
            raise ValueError("min_ 不能大于 max_")

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
        差集
        :param other:
        :return:
        """
        ms = self.members() - other.members()
        return SortedSet({m: self[m] for m in ms})

    __sub__ = difference

    def get(self, member, default=None):
        """
        获取某个成员的分数
        :param member:
        :param default:
        :return:
        """
        return self._data.get(member, default)

    def incr(self, member: str, amount: float) -> float:
        """
        递增某个成员的分数，如成员不存在，则以该分数初始化成员-分数键值对
        :param member:
        :param amount: 增加的分数值
        :return:
        """
        if member in self:
            self[member] += amount
        else:
            self[member] = amount

        return self[member]

    def intersection(self, other: "SortedSet"):
        """
        交集
        :param other:
        :return:
        """
        ms = self.members() & other.members()
        return SortedSet({m: self[m] for m in ms})

    __and__ = intersection

    def pop(self, member, default=None):
        """
        移除并返回某个成员的分数
        :param member:
        :param default:
        :return:
        """
        return self._data.pop(member, default)

    def popitem(self, last=True):
        """
        从头部或者尾部弹出 item
        :param last:
        :return:
        """
        return self._data.popitem(last=last)

    def randmember(self, count: int = 1, unique=True):
        """
        随机获取成员
        :param count: 获取的成员数量，默认 1
        :param unique: 获取的成员是否能重复
        :return:
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
        根据索引范围或分数范围获取相应范围的 成员-分数 键值对
        :param start:
        :param end:
        :param min_:
        :param max_:
        :param desc:
        :return:
        """

        if desc:
            sorted_items = sorted(self, key=lambda x: (x[1], x[0]), reverse=True)
        else:
            sorted_items = list(self)

        # 过滤分数范围
        if min_ is not None and max_ is not None:
            sorted_items = [(m, s)
                            for m, s in sorted_items
                            if min_ <= s <= max_]

        # 处理索引, Redis 是包含右边界的
        if end < 0:
            end = len(sorted_items) + end + 1
        else:
            end += 1

        if start > end:
            return []

        return sorted_items[start:end]

    def rank(self, member: str, desc=False) -> int | None:
        """
        获取某个成员的排名
        :param member:
        :param desc: 是否按降序排名
        :return:
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
        并集
        :param other:
        :return:
        """
        # add scores
        temp = {**other._data}
        for member in self.members():
            if member in temp:
                temp[member] += self[member]
        return SortedSet({**self._data, **temp})

    __or__ = union
