import inspect
import json
import random
import threading
import time
import weakref
from collections import OrderedDict
from fnmatch import fnmatchcase
from functools import reduce
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Union
from pathlib import Path
from urllib import parse

from litedis import AOFFsyncStrategy, BaseLitedis, DataType, PersistenceType
from litedis.aof import AOF, collect_command_to_aof
from litedis.rdb import RDB
from litedis.expiry import Expiry
from litedis.typing import Number, StringableT
from litedis.utils import find_index_from_left, find_index_from_right, list_or_args


class SortedSet(Iterable):

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

    def add(self, item: (str, Number)):
        """
        添加元素到有序集合，不存在则添加，存在则更新
        :param item: 成员+分数 元组
        """
        m, s = item
        self[m] = s

    def count(self, min_: Number, max_: Number) -> int:
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
        ms = self.members() - other.members()
        return SortedSet({m: self[m] for m in ms})

    __sub__ = difference

    def get(self, member, default=None):
        return self._data.get(member, default)

    def incr(self, member: str, amount: Number) -> Number:
        if member in self:
            self[member] += amount
        else:
            self[member] = amount

        return self[member]

    def intersection(self, other: "SortedSet"):
        ms = self.members() & other.members()
        return SortedSet({m: self[m] for m in ms})

    __and__ = intersection

    def pop(self, member, default=None):
        return self._data.pop(member, default)

    def popitem(self, last=True):
        return self._data.popitem(last=last)

    def randmember(self, count: int = 1, unique=True):
        if unique:
            return random.sample(list(self), count)
        else:
            return random.choices(list(self), k=count)

    def range(self,
              start: int,
              end: int,
              min_: Optional[Number] = None,
              max_: Optional[Number] = None,
              desc: bool = False,
              ) -> List:

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

    def rank(self, member: str, desc=False) -> Optional[int]:
        if member not in self:
            return None

        if desc:
            return list(reversed(self.members())).index(member)
        else:
            return list(self.members()).index(member)

    score = get

    def union(self, other: "SortedSet"):
        ms = self.members() | other.members()
        ms_mapping = {**self._data, **other._data}
        return SortedSet({m: ms_mapping[m] for m in ms})

    __or__ = union


class _SingletonMeta(type):
    """
    单例元类，确保一个类只有一个实例

    主要给 Litedis 创建单例使用

    使用 '/path/db' 作为单一实例依据，即同一个数据库只能创建一个单例
    """
    _instances = weakref.WeakValueDictionary()
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        # 只能给Litedis使用
        if cls is not Litedis:
            raise TypeError(f"该元类只能给 {Litedis.__name__} 使用")

        # 如果禁止，则不创建单例
        singleton = kwargs.get('singleton', None)
        if singleton is False:
            return super().__call__(*args, **kwargs)

        with cls._lock:
            connection_string = None
            # 如果 args 有值，则第一个位置参数必然是 connection_string
            if args:
                connection_string = args[0]
            # args 没有，则从关键字参数里获取
            if not connection_string:
                connection_string = kwargs.get("connection_string", None)
            # kwargs 也没有，代表没有使用 connection_string 参数，获取 data_dir 和 db_name
            if not connection_string:
                litedis_init_sign = inspect.signature(Litedis.__init__)
                data_dir = kwargs.get("data_dir", None)
                if not data_dir:
                    data_dir = litedis_init_sign.parameters.get("data_dir", None)
                db_name = kwargs.get("db_name", None)
                if not db_name:
                    db_name = litedis_init_sign.parameters.get("db_name", None)
                if data_dir and db_name:
                    connection_string = f"litedis:///{data_dir.lstrip('./|/').rstrip('/')}/{db_name}"

            if not connection_string:
                raise ValueError("未知错误，请检查 connection_string,data_dir,db_name参数")

            if connection_string not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[connection_string] = instance

        return cls._instances[connection_string]


class BasicKey(BaseLitedis):

    def _check_string_type(self, name):
        if self.data_types[name] != DataType.STRING:
            raise TypeError(f"{name}的数据类型 不是字符串！")

    def _set_string_value(self,
                          name: str,
                          value: StringableT,
                          exat: Union[int, None] = None,
                          check_string_type=True):
        if check_string_type and name in self.data:
            self._check_string_type(name)

        self.data[name] = value
        self.data_types[name] = DataType.STRING
        if exat:
            self.expires[name] = exat

    @collect_command_to_aof
    def append(self, key: str, value: StringableT) -> int:
        """
        将一个字符串值追加到已存在的键的末尾

        返回追加后的字符串的长度
        """
        with self.db_lock:
            self._set_string_value(key,
                                   str(self.data[key]) + str(value))

            return len(self.data[key])

    @collect_command_to_aof
    def copy(
            self,
            source: str,
            destination: str,
            replace: bool = False,
    ) -> bool:
        """
        复制一个键及其值`source`到另一个键`destination`

        返回 True 表示复制成功，返回 False 表示源键不存在
        """
        with self.db_lock:
            if source not in self.data:
                return False

            if destination in self.data and not replace:
                return False

            self._set_string_value(destination,
                                   self.data[source],
                                   exat=self.expires.get(source, None))

            return True

    @collect_command_to_aof
    def decrby(self, name: str, amount: int = 1) -> int:
        """
        将指定键的整数值减少指定的增量。

        如果键不存在，DECRBY 命令会将键的值初始化为 0，然后再进行递减操作

        返回递减操作后，键的当前值
        """
        with self.db_lock:
            return self._incrby(name, -amount)

    @collect_command_to_aof
    def delete(self, *names: str) -> int:
        """
        删除一个或多个键

        返回回被删除的键的数量
        """
        with self.db_lock:
            num = 0
            exists_names = [n for n in names if n in self.data]

            for name in exists_names:
                self.data.pop(name, None)
                self.data_types.pop(name, None)
                self.expires.pop(name, None)
                num += 1

            return num

    def __delitem__(self, name: str):
        self.delete(name)

    def dump(self, name: str) -> Optional[str]:
        """
        序列化给定键的值并返回序列化后的字符串

        返回序列化后的字符串。如果键不存在，则返回 None
        """
        with self.db_lock:
            if name not in self.data:
                return None

            return json.dumps(self.data[name])

    def exists(self, *names: str) -> int:
        """
        检查一个或多个键是否存在

        返回存在的键的数量。如果没有键存在，则返回 0。
        """
        with self.db_lock:
            num = 0
            for name in names:
                if name in self.data:
                    num += 1
            return num

    __contains__ = exists

    def expire(
            self,
            name: str,
            seconds: int,
            nx: bool = False,
            xx: bool = False,
            gt: bool = False,
            lt: bool = False,
    ) -> bool:
        """
        设置一个键的过期时间

        可选参数:
            NX -> 未设置过期时间的才能设置

            XX -> 有设置过期时间的才能设置

            GT -> 仅在设置的过期时间大于当前的过期时间时，才能设置。如果当前没有过期时间，则会设置过期时间。

            LT -> 仅在设置的过期时间小于当前的过期时间时，才能设置。如果当前没有过期时间，则不会设置过期时间。

        返回 True 表示成功设置过期时间，返回 False 表示键不存在或过期时间未设置
        """
        when = seconds + int(time.time())
        return self.expireat(name, when, nx=nx, xx=xx, gt=gt, lt=lt)

    @collect_command_to_aof
    def expireat(
            self,
            name: str,
            when: int,
            nx: bool = False,
            xx: bool = False,
            gt: bool = False,
            lt: bool = False,
    ) -> bool:
        """
        设置一个键在特定时间点过期

        when: 键的过期时间，以 Unix 时间戳（自 1970 年 1 月 1 日以来的秒数）表示。

        可选参数:
            NX -> 未设置过期时间的才能设置

            XX -> 有设置过期时间的才能设置

            GT -> 仅在设置的过期时间大于当前的过期时间时，才能设置。如果当前没有过期时间，则会设置过期时间。

            LT -> 仅在设置的过期时间小于当前的过期时间时，才能设置。如果当前没有过期时间，则不会设置过期时间。

        返回 True 表示成功设置过期时间，返回 False 表示键不存在或过期时间未设置。
        """
        with self.db_lock:
            if name not in self.data:
                return False

            if nx and name in self.expires:
                return False

            if xx and name not in self.expires:
                return False

            if name in self.expires:
                if gt and when <= self.expires[name]:
                    return False
                if lt and when >= self.expires[name]:
                    return False

            self.expires[name] = when
            return True

    def expiretime(self, name: str) -> int:
        """
        获取指定键的过期时间

        返回键的过期时间（以 Unix 时间戳的形式），如果键不存在或没有设置过期时间，则返回 -1。
        """
        with self.db_lock:
            if name not in self.data:
                return -1

            if name not in self.expires:
                return -1

            return int(self.expires[name])

    def get(self, name: str) -> Optional[StringableT]:
        """
        返回键“name”的值，

        如果键不存在，则返回None
        """
        with self.db_lock:
            if name not in self.data:
                return None

            self._check_string_type(name)

            return self.data[name]

    def __getitem__(self, name: str):
        """
        返回键“name”的值，

        如果键不存在则引发KeyError。
        """
        value = self.get(name)

        if value is not None:
            return value

        raise KeyError(name)

    def _incrby(self, name: str, amount: Number = 1) -> Number:
        if name not in self.data:
            self._set_string_value(name, 0, check_string_type=False)
        else:
            self._check_string_type(name)

        self.data[name] = type(amount)(self.data[name]) + amount
        return self.data[name]

    @collect_command_to_aof
    def incrby(self, name: str, amount: int = 1) -> int:
        """
        将“key”的值增加“amount”。

        如果键不存在，值将被初始化为“amount”。
        """
        with self.db_lock:
            return self._incrby(name, amount)

    incr = incrby

    @collect_command_to_aof
    def incrbyfloat(self, name: str, amount: float = 1.0) -> float:
        """
        将键“name”的值按浮点数“amount”增加。

        如果键不存在，值将被初始化为“amount”。
        """
        with self.db_lock:
            return self._incrby(name, amount)

    def keys(self, pattern: str = "*") -> List[str]:
        """
        返回与“pattern”匹配的键的列表。
        """
        with self.db_lock:
            # 这里取个巧
            if pattern == "*":
                return list(self.data.keys())

            return [key
                    for key in self.data.keys()
                    if fnmatchcase(key, pattern)]

    def mget(self, keys: Union[str, Iterable[str]], *args: str) -> List[StringableT]:
        """
        返回与“keys”的顺序相同的值列表。
        """

        with self.db_lock:
            args = list_or_args(keys, args)

            return [self.data[arg]
                    for arg in args
                    if arg in self.data]

    @collect_command_to_aof
    def mset(self, mapping: Mapping[str, StringableT]) -> bool:
        """
        根据映射设置键/值。映射是键/值对的字典。

        如果键已存在，将被覆盖，不同数据类型的也一样
        """
        with self.db_lock:
            for k, v in mapping.items():
                self._set_string_value(k, v, check_string_type=False)

            return True

    @collect_command_to_aof
    def msetnx(self, mapping: Mapping[str, StringableT]) -> bool:
        """
        如果没有任何键已经设置，根据映射设置键/值。

        返回一个布尔值，指示操作是否成功。
        """
        with self.db_lock:
            # 相交key集合
            intersection = mapping.keys() & self.data.keys()
            if len(intersection) > 0:
                return False

            for k, v in mapping.items():
                self._set_string_value(k, v, check_string_type=False)

            return True

    @collect_command_to_aof
    def persist(self, name: str) -> bool:
        """
        删除键“name”的到期时间。

        如果键存在并且成功移除过期时间，返回 True。
        如果键不存在，或者键没有设置过期时间，返回 False。
        """
        with self.db_lock:
            if name not in self.data:
                return False

            if name not in self.expires:
                return False

            self.expires.pop(name, None)
            return True

    def randomkey(self) -> str:
        """
        返回一个随机键的名称
        """
        return random.choice(list(self.data.keys()))

    @collect_command_to_aof
    def rename(self, src: str, dst: str) -> bool:
        """
        将键 “src” 重命名为 “dst”

        如果重命名成功，返回 True。

        如果 “src” 不存在，将触发异常。

        如果 “dst” 存在，将被覆盖
        """
        with self.db_lock:
            if src not in self.data:
                raise AttributeError(f"键: {src} 不存在与数据库")

            self.data[dst] = self.data.pop(src)
            self.data_types[dst] = self.data_types.pop(src)
            if src in self.expires:
                self.expires[dst] = self.expires.pop(src)

            return True

    @collect_command_to_aof
    def renamenx(self, src: str, dst: str):
        """
        如果 “dst” 不存在，则将键 “src” 重命名为 “dst”

        如果重命名成功，返回 True。

        如果 “src” 不存在，将触发异常。
        """
        with self.db_lock:
            if dst in self.data:
                return

            if src not in self.data:
                raise AttributeError(f"键: {src} 不存在与数据库")

            self.data[dst] = self.data.pop(src)
            self.data_types[dst] = self.data_types.pop(src)
            if src in self.expires:
                self.expires[dst] = self.expires.pop(src)

    @collect_command_to_aof
    def set(
            self,
            name: str,
            value: StringableT,
            ex: Union[int, None] = None,
            nx: bool = False,
            xx: bool = False,
            get: bool = False,
            exat: Union[int, None] = None,
    ) -> Union[bool, StringableT]:
        """
        将键 ``name`` 的值设置为 ``value``

        ``ex`` 在键 ``name`` 上设置一个过期标志，过期时间为 ``ex`` 秒。

        ``nx`` 如果设置为 True，则仅当键不存在时，才将键 ``name`` 的值设置为 ``value``。

        ``xx`` 如果设置为 True，则仅当键存在时，才将键 ``name`` 的值设置为 ``value``。

        ``get`` 如果为 True，则将键 ``name`` 的值设置为 ``value``，并返回键 ``name`` 的旧值，或者如果键不存在，则返回 None。

        ``exat`` 在键 ``name`` 上设置一个过期标志，过期时间为 ``ex`` 秒，指定为unix时间。
        """
        with self.db_lock:
            if nx and name in self.data:
                return False

            if xx and name not in self.data:
                return False

            if name in self.data:
                self._check_string_type(name)

            expire = exat
            if ex is not None:
                expire = time.time() + ex

            old_value = self.data.get(name, None)

            self._set_string_value(name, value, exat=expire)

            if get:
                return old_value

            return True

    def __setitem__(self, name: str, value: str):
        self.set(name, value)

    def strlen(self, name: str) -> int:
        """
        返回键 ``name`` 的值中存储的字符串长度

        如果键不存在，返回 0
        """
        with self.db_lock:
            if name not in self.data:
                return 0

            return len(self.data[name])

    def substr(self, name: str, start: int, end: int = -1) -> StringableT:
        """
        返回键 ``name`` 的字符串值的子串。

        ``start`` 和 ``end`` 指定要返回的子串的部分。

        如果键不存在，返回空字符串
        """
        with self.db_lock:
            if name not in self.data:
                return ""

            self._check_string_type(name)

            value = str(self.data[name])
            return value[start:end]

    def ttl(self, name: str) -> int:
        """
        返回键 ``name`` 将过期的秒数

        如果键不存在，返回 -2。

        如果键存在但没有设置过期时间，返回 -1。
        """
        with self.db_lock:
            if name not in self.data:
                return -2

            if name not in self.expires:
                return -1

            return max(0, round(self.expires[name] - time.time()))

    def type(self, name: str) -> str:
        """
        返回键 ``name`` 的类型

        如果键不存在，返回 ``none``
        """
        with self.db_lock:
            if name not in self.data:
                return "none"

            return self.data_types[name]


class ListType(BaseLitedis):

    def _check_list_type(self, name):
        if self.data_types[name] != DataType.LIST:
            raise TypeError(f"{name}的数据类型 不是列表！")

    def lindex(self, name: str, index: int) -> Optional[StringableT]:
        """
        返回列表“name”中位置“index”的项。

        支持负索引。

        如果索引超出范围或键不存在，返回 None
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return None

            self._check_list_type(name)

            if abs(index) >= len(list_):
                return None

            return list_[index]

    @collect_command_to_aof
    def linsert(self, name: str, where: str, refvalue: StringableT, value: StringableT) -> int:
        """
        在列表 name 中以 refvalue 为基准的 where 位置（BEFORE/AFTER）插入 value 。

        成功时返回列表的新长度，如果 refvalue 不在列表中则返回-1。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return -1

            self._check_list_type(name)

            if refvalue not in list_:
                return -1

            ref_index = list_.index(refvalue)
            if where.lower() == "after":
                ref_index += 1

            list_.insert(ref_index, value)

            return len(list_)

    def llen(self, name: str) -> int:
        """
        返回列表“name”的长度。

        如果键不存在，返回 0。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return 0

            self._check_list_type(name)

            return len(list_)

    @collect_command_to_aof
    def lpop(self, name: str, count: Optional[int] = None) -> Union[StringableT, List, None]:
        """
        移除并返回列表 name 的前面几个元素。

        默认情况下，命令从列表的开头弹出一个元素。

        当提供可选的 count参数时，返回最多count个元素。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return None

            self._check_list_type(name)

            if not list_:
                return None

            if count is None:
                return list_.pop(0)

            num = min(count, len(list_))
            return [list_.pop(0) for _ in range(num)]

    @collect_command_to_aof
    def lpush(self, name: str, *values: StringableT) -> int:
        """
        将 values 推送到列表 name 的头部。

        返回插入后列表的长度
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is not None:
                self._check_list_type(name)
            else:
                list_ = self.data[name] = []
                self.data_types[name] = DataType.LIST

            for v in values:
                list_.insert(0, v)

            return len(list_)

    @collect_command_to_aof
    def lpushx(self, name: str, *values: StringableT) -> int:
        """
        如果 name 存在，则将 value 推送到列表 name 的头部。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return 0

            self._check_list_type(name)

            for v in values:
                list_.insert(0, v)

            return len(list_)

    def lrange(self, name: str, start: int, end: int) -> List:
        """
        返回列表 name 在位置 start 和 end 之间的切片。

        start 和 end 可以是负数，就像Python的切片表示法一样。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return []

            self._check_list_type(name)

            # 处理索引, Redis 是包含右边界的
            if end < 0:
                end = len(list_) + end + 1
            else:
                end += 1

            return list_[start:end]

    @collect_command_to_aof
    def lrem(self, name: str, count: int, value: str) -> int:
        """
        从存储在 name 中的列表中删除与 value 相等的元素的第一个 count 次出现。

        count 参数影响操作的方式：
            count > 0：从头到尾移动，删除与 value 相等的元素。

            count < 0：从尾到头移动，删除与 value 相等的元素。

            count = 0：删除所有与 value 相等的元素。

        返回实际删除的元素数量。
        """

        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return 0

            self._check_list_type(name)

            # 小于 0，从右查找，否则从左查找
            if count < 0:
                find_index = find_index_from_right
            else:
                find_index = find_index_from_left

            num = 0
            # 一直删除，直到达到 count 值或者全部删完
            while index := find_index(list_, value) >= 0:
                list_.pop(index)
                num += 1
                if abs_count := abs(count) > 0:
                    if num >= abs_count:
                        break
            return num

    @collect_command_to_aof
    def lset(self, name: str, index: int, value: str) -> bool:
        """
        将列表 name 中位置 index 的元素设置为 value 。

        返回 True 表示成功设置元素的值。

        如果 name 不存在或者索引超出范围，Redis 将返回错误。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                raise ValueError(f"name: {name} 不存在相应的值")

            self._check_list_type(name)

            if abs(index) > len(list_):
                raise IndexError(f"index: {index} 超出索引范围")

            list_[index] = value

            return True

    @collect_command_to_aof
    def ltrim(self, name: str, start: int, end: int) -> bool:
        """
        修剪列表“name”，删除不在“start”和“end”之间的所有值。

        “start”和“end”可以是负数。

        返回 True 表示成功修剪列表。

        如果键不存在，列表将被创建为空列表。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                list_ = self.data[name] = []
                self.data_types[name] = DataType.LIST
            else:
                self._check_list_type(name)

            # 处理索引, Redis 是包含右边界的
            if end < 0:
                end = len(list_) + end + 1
            else:
                end += 1

            self.data[name] = list_[start:end]

            return True

    @collect_command_to_aof
    def rpop(self, name: str, count: Optional[int] = None) -> Union[StringableT, List, None]:
        """
        移除并返回列表 name 的最后元素。

        默认情况下，命令从列表的末尾弹出一个元素。

        当提供可选的 count 参数时，将根据列表的长度返回最多count个元素。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return None

            self._check_list_type(name)

            if count is None:
                return list_.pop()

            num = min(count, len(list_))
            return [list_.pop() for _ in range(num)]

    @collect_command_to_aof
    def rpush(self, name: str, *values: StringableT) -> int:
        """
        将 values 添加到列表 name 的尾部。

        返回插入后列表的长度。

        如果指定的键不存在，RPUSH 会创建一个新的列表。

        如果键的值不是列表类型，Redis 将返回错误。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                list_ = self.data[name] = []
                self.data_types[name] = DataType.LIST
            else:
                self._check_list_type(name)

            list_.extend(values)

            return len(list_)

    @collect_command_to_aof
    def rpushx(self, name: str, *values: StringableT) -> int:
        """
        如果 name 存在，则将 value 添加到列表 name 的尾部。

        返回插入后列表的长度。如果指定的列表不存在，则返回 0。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return 0

            self._check_list_type(name)

            list_.extend(values)

            return len(list_)

    @collect_command_to_aof
    def lsort(
            self,
            name: str,
            key: Optional[Callable] = None,
            desc: bool = False
    ) -> List[StringableT]:
        """
        对列表 name 进行排序并返回。

        key 自定义排序

        desc 是否反转排序

        返回排序后的元素列表。

        如果指定的键不存在，返回空列表。
        """
        with self.db_lock:
            list_ = self.data.get(name, None)
            if list_ is None:
                return []

            self._check_list_type(name)

            self.data[name].sort(key=key, reverse=desc)

            return self.data[name]


class SetType(BaseLitedis):

    def _check_set_type(self, name):
        if self.data_types[name] != DataType.SET:
            raise TypeError(f"{name}的数据类型 不是集合！")

    @collect_command_to_aof
    def sadd(self, name: str, *values: StringableT) -> int:
        """
        添加 ``value(s)`` 到 ``name`` 集合

        如果集合不存在，则会创建一个新的集合。

        该命令返回添加到集合中的新成员的数量，不包括已经存在于集合中的成员。
        """
        with self.db_lock:
            set_ = self.data.get(name, None)
            if set_ is None:
                set_ = self.data[name] = set()
                self.data_types[name] = DataType.SET
            else:
                self._check_set_type(name)

            set_to_add = set(values) - set_

            self.data[name] = set_ | set_to_add

            return len(set_to_add)

    def scard(self, name: str) -> int:
        """
        返回集合``name``中的元素数量

        如果集合不存在，返回值将是 0
        """
        with self.db_lock:
            set_ = self.data.get(name, None)
            if set_ is None:
                return 0

            self._check_set_type(name)

            return len(set_)

    def sdiff(self, keys: List[str], *args: str) -> Set[StringableT]:
        """
        返回指定的``keys``的集合的差集
        """
        with self.db_lock:
            args = list_or_args(keys, args)

            for name in args:
                if name in self.data:
                    self._check_set_type(name)

            set_list = [self.data.get(name, set()) for name in args]

            return reduce(lambda s1, s2: s1 - s2, set_list)

    def sinter(self, keys: List[str], *args: str) -> Set[StringableT]:
        """
        返回指定的``keys``的集合的交集
        """
        with self.db_lock:
            args = list_or_args(keys, args)

            for name in args:
                if name in self.data:
                    self._check_set_type(name)

            set_list = [self.data.get(name, set()) for name in args]

            return reduce(lambda s1, s2: s1 & s2, set_list)

    def sismember(self, name: str, value: StringableT) -> bool:
        """
        返回``value``是否是集合``name``的成员：

        - 如果``value``是集合的成员，则返回 True。
        - 如果``value``不是集合的成员，或者键不存在，则返回 False。
        """
        with self.db_lock:
            set_ = self.data.get(name, None)
            if set_ is None:
                return False

            self._check_set_type(name)

            return value in set_

    def smembers(self, name: str) -> Set:
        """
        返回集合``name``的所有成员

        如果集合不存在，SMEMBERS 将返回一个空的列表。
        """
        with self.db_lock:
            set_ = self.data.get(name, None)
            if set_ is None:
                return set()

            self._check_set_type(name)

            return set_

    def smismember(self, name: str, values: List[StringableT], *args: str) -> List[bool]:
        """
        判断每个在``values``中的值是否是集合``name``的成员，以``values``的顺序返回一个列表

        - 如果值是集合的成员，则返回 True。
        - 如果值不是集合的成员，或者键不存在，则返回 False。

        如果集合``name``不存在，返回一个空数组。
        """
        with self.db_lock:
            set_ = self.data.get(name, None)
            if set_ is None:
                return []

            self._check_set_type(name)

            args = list_or_args(values, args)
            return [v in set_ for v in args]

    @collect_command_to_aof
    def smove(self, src: str, dst: str, value: StringableT) -> bool:
        """
        原子地将``value``从集合``src``移动到集合``dst``。

        如果 value 成功地从 source 集合中移除并添加到 destination 集合中，命令返回 True。

        如果 source 集合不存在，返回 False。

        如果 value 不在 source 集合中，命令返回 False。

        如果 destination 集合不存在，自动创建它。
        """
        with self.db_lock:
            set_src = self.data.get(src, None)
            if set_src is None:
                return False

            self._check_set_type(src)

            if value not in set_src:
                return False

            set_dst = self.data.get(dst, None)
            if set_dst is None:
                set_dst = self.data[dst] = set()
                self.data_types[dst] = DataType.SET
            else:
                self._check_set_type(dst)

            set_src.remove(value)
            set_dst.add(value)

            return True

    @collect_command_to_aof
    def spop(self, name: str, count: Optional[int] = None) -> Union[StringableT, List, None]:
        """
        从集合``name``中移除并返回一个随机成员

        集合``name``不存在或为空，返回 None
        """
        with self.db_lock:
            set_ = self.data.get(name, None)
            if set_ is None:
                return None

            self._check_set_type(name)

            if not set_:
                return None

            if count is None:
                return set_.pop()

            num = min(count, len(set_))
            return [set_.pop() for _ in range(num)]

    def srandmember(self, name: str, number: Optional[int] = None) -> Union[StringableT, List, None]:
        """
        如果``number``为 None，则返回集合``name``的一个随机成员。

        如果``number``不为 None，则返回集合``name``的``number``个随机成员的列表。

        如果 number 是正数，返回指定数量的随机不同元素；如果是负数，则返回指定数量的随机元素，可以重复。

        如果集合不存在或为空，返回 None
        """
        with self.db_lock:
            set_ = self.data.get(name, None)
            if set_ is None:
                return None

            self._check_set_type(name)

            if not set_:
                return None

            if number is None:
                return random.sample(set_, 1)[0]

            if number == 0:
                return []
            elif number < 0:
                return random.choices(list(set_), k=abs(number))
            else:
                return random.sample(set_, number)

    @collect_command_to_aof
    def srem(self, name: str, *values: StringableT) -> int:
        """
        从集合``name``中移除``values``

        返回被成功移除的成员数量。如果指定的成员在集合中不存在，返回值仍然是成功移除的成员数量。
        """
        with self.db_lock:
            set_ = self.data.get(name, None)
            if set_ is None:
                return 0

            self._check_set_type(name)

            num = 0
            for v in values:
                if v in set_:
                    set_.remove(v)
                    num += 1

            return num

    def sunion(self, keys: List[StringableT], *args: StringableT) -> Set[StringableT]:
        """
        返回指定的``keys``的集合的并集
        """
        with self.db_lock:
            args = list_or_args(keys, args)

            for name in args:
                if name in self.data:
                    self._check_set_type(name)

            set_list = [self.data.get(name, set()) for name in args]

            return reduce(lambda s1, s2: s1 | s2, set_list)


class SortedSetType(BaseLitedis):

    def _check_sortedset_type(self, name):
        if self.data_types[name] != DataType.SortedSet:
            raise TypeError(f"{name}的数据类型 不是有序集合！")

    @collect_command_to_aof
    def zadd(
            self,
            name: str,
            mapping: Mapping[str, StringableT],
            nx: bool = False,
            xx: bool = False,
            gt: bool = False,
            lt: bool = False,
    ) -> int:
        """
        将任意数量的元素-名称、分数对设置到键``name``。对是指定为元素名称键到分数值的字典。

        ``nx`` 强制ZADD只创建新元素，而不更新已存在元素的分数。

        ``xx`` 强制ZADD只更新已存在元素的分数。新元素不会被添加。

        ``LT`` 仅在新分数小于当前分数时更新现有元素。此标志不会阻止添加新元素。

        ``GT`` 仅在新分数大于当前分数时更新现有元素。此标志不会阻止添加新元素。

        ZADD的返回值根据指定的模式而变化。没有选项时，ZADD返回添加到有序集合的新元素数量。

        ``NX``、``LT``和``GT``是互斥的选项。
        """
        with self.db_lock:
            if not mapping:
                raise ValueError("ZADD需要至少一个元素/分数对")

            if nx and xx:
                raise ValueError("ZADD只允许'nx'或'xx'，不能同时使用")
            if gt and lt:
                raise ValueError("ZADD只允许'gt'或'lt'，不能同时使用")
            if nx and (gt or lt):
                raise ValueError("只能定义'nx'、'lt'或'gr'中的一个。")

            zset = self.data.get(name, None)

            if zset is None:
                zset = self.data[name] = SortedSet()
                self.data_types[name] = DataType.SortedSet
            else:
                self._check_sortedset_type(name)

            add_num = 0
            for mem, score in mapping.items():
                if mem in zset:
                    if nx:
                        continue
                    # 已存在，更新，但要排除 lt 或 gt 的限制
                    if lt and score >= zset[mem]:
                        continue
                    if gt and score <= zset[mem]:
                        continue
                    zset.add((mem, score))
                else:
                    if xx:
                        continue
                    # 不存在，添加
                    zset.add((mem, score))
                    add_num += 1

            return add_num

    def zcard(self, name: str) -> int:
        """
        返回有序集合``name``中的元素数量

        如果该有序集合不存在，返回值将是 0
        """
        with self.db_lock:
            zset = self.data.get(name, None)

            if zset is None:
                return 0

            self._check_sortedset_type(name)

            return len(zset)

    def zcount(self, name: str, min_: Number, max_: Number) -> int:
        """
        返回键``name``中分数在``min``和``max``之间的元素数量。
        """
        with self.db_lock:
            zset = self.data.get(name, None)

            if zset is None:
                return 0

            self._check_sortedset_type(name)

            return zset.count(min_, max_)

    def zdiff(self, keys: List[str], withscores: bool = False) -> List:
        """
        返回在``keys``中提供的第一个和所有后续输入有序集合之间的差异。
        """
        with self.db_lock:
            return self._sortedset_reduce_openration(
                lambda s1, s2: s1 - s2,
                keys=keys,
                withscores=withscores)

    @collect_command_to_aof
    def zincrby(self, name: str, amount: Number, value: str) -> Number:
        """
        将有序集合``name``中的``value``的分数增加``amount``

        如果成员在有序集合中存在，返回成员的新分数。

        如果成员在有序集合中不存在，ZINCRBY 会将该成员添加到集合中，并将其分数设置为增量值（即 increment 的值）。
        """
        with self.db_lock:
            zset = self.data.get(name, None)

            if zset is None:
                zset = self.data[name] = SortedSet()
                self.data_types[name] = DataType.SortedSet
            else:
                self._check_sortedset_type(name)

            return zset.incr(member=value, amount=amount)

    def zinter(self, keys: List[str], withscores: bool = False) -> List:
        """
        返回由``keys``指定的多个有序集合的交集。
        """
        with self.db_lock:
            return self._sortedset_reduce_openration(
                lambda s1, s2: s1 & s2,
                keys=keys,
                withscores=withscores)

    def zintercard(self, numkeys: int, keys: List[str], limit: int = 0) -> int:
        """
        返回由``keys``指定的多个有序集合的交集的基数。

        numkeys: 要计算交集的有序集合个数

        limit: 可选参数，限制要计算的元素数量
        """
        with self.db_lock:
            for name in keys:
                if name in self.data:
                    self._check_sortedset_type(name)

            zsets = [self.data.get(n, SortedSet()) for n in keys]

            # 计算交集
            numkeys = min(numkeys, len(keys))
            inter = zsets[0]
            for num in range(1, numkeys):
                inter = inter & zsets[num]
                if limit != 0 and len(inter) >= limit:
                    return limit

            return len(inter)

    def _zpopmaxmin(self, type_, name: str, count: int = 1) -> List:

        if count < 1:
            raise ValueError(f"count:{count} 不能小于 1")

        zset = self.data.get(name, None)

        if zset is None:
            return []

        self._check_sortedset_type(name)

        if count == 1:
            pop_lists = [zset.popitem(last=type_ == "max")]
        else:
            count = min(count, len(zset))
            pop_lists = [zset.popitem(last=type_ == "max") for _ in range(count)]

        # 展平列表再返回
        return [i
                for t in pop_lists
                for i in t]

    @collect_command_to_aof
    def zpopmax(self, name: str, count: int = 1) -> List:
        """
        从有序集合``name``中删除并返回分数最高的成员

        count: 可选参数，指定要返回的成员数量，默认为1

        返回被删除的成员和它们的分数。如果键不存在，返回空数组。

        返回格式为: member1 score1 [member2 score2 ...]
        """
        with self.db_lock:
            return self._zpopmaxmin(type_="max", name=name, count=count)

    @collect_command_to_aof
    def zpopmin(self, name: str, count: int = 1) -> List:
        """
        移除并返回有序集合``name``中分数最低的成员

        count: 可选参数，指定要返回的成员数量，默认为1

        返回被删除的成员和它们的分数。如果键不存在，返回空数组。

        返回格式为: member1 score1 [member2 score2 ...]
        """
        with self.db_lock:
            return self._zpopmaxmin(type_="min", name=name, count=count)

    def zrandmember(self, key: str, count: int = 1, withscores: bool = False) -> Union[List, StringableT, None]:
        """
        从有序集合中随机返回一个或多个成员。该命令在 Redis 6.2.0 版本中新增。

        count: 可选参数，指定要返回的成员数量
            正数: 返回不重复的成员

            负数: 返回可能重复的成员

            不指定: 默认返回1个成员
        """

        with self.db_lock:
            zset = self.data.get(key, None)

            if zset is None:
                return None if count == 1 else []

            self._check_sortedset_type(key)

            mems = zset.randmember(count=abs(count), unique=count > 0)
            if withscores:
                # 展平
                return [ms
                        for item in mems
                        for ms in item]
            else:
                if len(mems) == 1:
                    return mems[0][0]
                else:
                    return [m for m, s in mems]

    @collect_command_to_aof
    def zmpop(
            self,
            keys: List[str],
            min_: Optional[bool] = False,
            max_: Optional[bool] = False,
            count: int = 1,
    ) -> List:
        """
        从第一个非空有序集合上弹出并返回分值最小或最大的成员。

        返回一个数组,包含以下内容:
            第一个元素是成功弹出元素的有序集合的键名
            第二个元素是一个数组,包含弹出的成员和分值对
        """
        if (min_ and max_) or (not min_ and not max_):
            raise ValueError(f"min_和 max_必须要有一个为真，但不能都为真")
        with self.db_lock:
            zset = None
            name = None
            for key in keys:
                zset = self.data.get(key, None)
                if zset:
                    name = key
                    break

            if zset is None:
                return []

            self._check_sortedset_type(name)

            type_ = "min" if min_ else "max"
            list_ = self._zpopmaxmin(type_=type_, name=name, count=count)
            return [name, list_]

    def _zrange(self,
                name: str,
                start: int,
                end: int,
                min_: Optional[Number] = None,
                max_: Optional[Number] = None,
                desc: bool = False,
                withscores: bool = False,
                ) -> List:

        zset = self.data.get(name, None)

        if zset is None:
            return []

        self._check_sortedset_type(name)

        result = zset.range(start, end, min_=min_, max_=max_, desc=desc)

        if not withscores:
            return [m for m, s in result]
        return result

    def zrange(self, name: str, start: int, end: int, withscores: bool = False, ) -> List:
        """
        获取有序集合(sorted set)中指定范围的成员。成员按照 score 值从小到大进行排序。

        返回指定范围的成员列表。如果使用 WITHSCORES 参数，则会同时返回成员的分数。
        """
        with self.db_lock:
            return self._zrange(name=name,
                                start=start,
                                end=end,
                                withscores=withscores)

    def zrevrange(self, name: str, start: int, end: int, withscores: bool = False, ) -> List:
        """
        获取有序集合(sorted set)中指定范围的成员。成员按照 score 值从大到小进行排序。

        返回指定范围的成员列表。如果使用 WITHSCORES 参数，则会同时返回成员的分数。
        """
        with self.db_lock:
            return self._zrange(name=name,
                                start=start,
                                end=end,
                                desc=True,
                                withscores=withscores)

    def zrangebyscore(
            self,
            name: str,
            min_: Number,
            max_: Number,
            start: Optional[int] = None,
            num: Optional[int] = None,
            withscores=False,
    ) -> List:
        """
        获取有序集合中指定分数区间内的成员。返回的成员是按照分数从小到大排序的。

        返回一个列表，包含指定分数范围的成员
        如果使用 WITHSCORES，则返回的列表中交替包含成员和分数
        如果 name 不存在或没有匹配的成员，返回空列表
        """
        with self.db_lock:
            if start is None:
                start = 0
            end = -1 if num is None else start + num

            return self._zrange(name=name,
                                start=start,
                                end=end,
                                min_=min_,
                                max_=max_,
                                desc=False,
                                withscores=withscores)

    def zrevrangebyscore(
            self,
            name: str,
            min_: Number,
            max_: Number,
            start: Optional[int] = None,
            num: Optional[int] = None,
            withscores=False,
    ) -> List:
        """
        获取有序集合中指定分数区间内的成员。返回的成员是按照分数从大到小排序的。

        返回一个列表，包含指定分数范围的成员
        如果使用 WITHSCORES，则返回的列表中交替包含成员和分数
        如果 name 不存在或没有匹配的成员，返回空列表
        """
        with self.db_lock:
            if start is None:
                start = 0
            end = -1 if num is None else start + num

            return self._zrange(name=name,
                                start=start,
                                end=end,
                                min_=min_,
                                max_=max_,
                                desc=True,
                                withscores=withscores)

    def zrank(self, name: str, value: StringableT) -> Optional[int]:
        """
        获取有序集合中成员的排名,按分数值从小到大排序。排名从0开始计算。

        如果成员存在于有序集合中,返回其排名(从0开始的整数)
        如果成员不存在或key不存在,返回 None
        """
        with self.db_lock:
            zset = self.data.get(name, None)

            if zset is None:
                return None

            self._check_sortedset_type(name)

            return zset.rank(value)

    @collect_command_to_aof
    def zrem(self, name: str, *values: StringableT) -> int:
        """
        从有序集合(sorted set)中删除一个或多个成员。

        返回整数值，表示成功删除的成员数量。
        如果 key 不存在，返回 0。
        """
        with self.db_lock:

            zset = self.data.get(name, None)
            if zset is None:
                return 0

            self._check_sortedset_type(name)

            num = 0
            for v in values:
                if v in zset:
                    zset.pop(v, None)
                    num += 1

            return num

    @collect_command_to_aof
    def zremrangebyscore(self, name: str, min_: Number, max_: Number) -> int:
        """
        移除有序集合``name``中分数在``min``和``max``之间的所有元素。

        返回被移除的元素数量。
        """
        with self.db_lock:
            list_ = self._zrange(name=name, start=0, end=-1, min_=min_, max_=max_)
            if not list_:
                return 0

            zset = self.data[name]
            num = 0
            for v in list_:
                zset.pop(v, None)
                num += 1

            return num

    def zrevrank(self, name: str, value: StringableT) -> Optional[int]:
        """
        获取有序集合中成员的排名,按分数值从大到小排序。排名从0开始计算。

        如果成员存在于有序集合中,返回其排名(从0开始的整数)
        如果成员不存在或key不存在,返回 None
        """
        with self.db_lock:
            zset = self.data.get(name, None)

            if zset is None:
                return None

            self._check_sortedset_type(name)

            return zset.rank(value, desc=True)

    def zscan(self, name: str, cursor: int = 0, count: int = 10):
        """
        遍历有序集合。

        这里和 Redis 的行为不同，将直接一次性扫描完成
        """
        with self.db_lock:
            zset = self.data.get(name, None)
            if zset is None:
                return 0, []

            self._check_sortedset_type(name)

            total = len(zset)
            start = cursor
            end = min(cursor + count, total)
            next_cursor = end if end < total else 0
            return next_cursor, list(zset.items())[start:end]

    def zscore(self, name: str, value: StringableT) -> Optional[float]:
        """
        获取有序集合中指定成员的分数

        如果指定的键不存在或成员不存在，返回 None。
        """
        with self.db_lock:

            zset = self.data.get(name, None)
            if zset is None:
                return None

            self._check_sortedset_type(name)

            return zset.get(value)

    def _sortedset_reduce_openration(
            self,
            reduce_openration,
            keys: List[str],
            withscores: bool = False
    ) -> List:
        """
        有序集合运算相同的部分
        """
        for name in keys:
            if name in self.data:
                self._check_sortedset_type(name)

        zsets = [self.data.get(n, SortedSet()) for n in keys]

        # 集合运算
        result = reduce(reduce_openration, zsets)

        if not withscores:
            result = [m for m, _ in result]
        else:
            result = list(result)

        return result

    def zunion(self, keys: List[str], withscores: bool = False, ) -> List:
        """
        返回由``keys``指定的多个有序集合的并集。
        """
        with self.db_lock:
            return self._sortedset_reduce_openration(
                lambda s1, s2: s1 | s2,
                keys=keys,
                withscores=withscores)

    def zmscore(self, key: str, members: List[str]) -> List[Optional[float]]:
        """
        返回存储在key的有序集合中与指定成员关联的分数。

        返回分数列表([9.0, None])
            如果指定的成员存在于有序集合中，返回该成员的分数。
            如果指定的成员不存在，对应位置的分数为 None。
        """
        with self.db_lock:
            zset = self.data.get(key, None)
            if zset is None:
                return [None for _ in members]

            self._check_sortedset_type(key)

            return [zset.get(m, None) for m in members]


class HashType(BaseLitedis):

    def _check_hash_type(self, name):
        if self.data_types[name] != DataType.HASH:
            raise TypeError(f"{name}的数据类型 不是哈希！")

    @collect_command_to_aof
    def hdel(self, name: str, *keys: str) -> int:
        """
        从哈希 name 中删除一个或多个字段

        返回被成功删除的字段数量。如果指定的哈希表或字段不存在，则返回 0。
        """
        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                return 0

            self._check_hash_type(name)

            num = 0
            for k in keys:
                if k in hash_:
                    hash_.pop(k)
                    num += 1

            return num

    def hexists(self, name: str, key: str) -> bool:
        """
        检查给定的哈希表中是否存在指定的字段。

        如果哈希表或字段不存在，返回 False。
        """
        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                return False

            self._check_hash_type(name)

            return key in hash_

    def hget(self, name: str, key: str) -> Optional[StringableT]:
        """
        获取哈希表中指定字段的值。

        如果哈希表或字段不存在，返回 None
        """
        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                return None

            self._check_hash_type(name)

            return hash_.get(key, None)

    def hgetall(self, name: str) -> Dict:
        """
        获取哈希表中所有字段及其对应值

        如果指定的 key 不存在，返回空字典。
        """
        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                return {}

            self._check_hash_type(name)

            return {**hash_}

    def _hincrby(self, name: str, key: str, amount: Number = 1) -> Number:
        hash_ = self.data.get(name, None)
        if hash_ is None:
            hash_ = self.data[name] = {}
            self.data_types[name] = DataType.HASH
        else:
            self._check_hash_type(name)

        if key not in hash_:
            hash_[key] = 0

        hash_[key] += amount

        return hash_[key]

    @collect_command_to_aof
    def hincrby(self, name: str, key: str, amount: int = 1) -> int:
        """
        对哈希表中指定字段的整数值进行增量操作

        如果哈希表或字段不存在，会先初始化为 0 再增量
        """
        with self.db_lock:
            return self._hincrby(name, key, amount)

    @collect_command_to_aof
    def hincrbyfloat(self, name: str, key: str, amount: float = 1.0) -> float:
        """
        对哈希表中指定字段的整数值进行增量操作。

        如果哈希表或字段不存在，会先初始化为 0 再增量
        """
        with self.db_lock:
            return self._hincrby(name, key, amount)

    def hkeys(self, name: str) -> List:
        """
        获取 哈希表中所有字段的名称

        返回一个包含哈希表中所有字段名称的数组。
        如果指定的键不存在，返回一个空数组。
        """
        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                return []

            self._check_hash_type(name)

            return list(hash_.keys())

    def hlen(self, name: str) -> int:
        """
        获取哈希表中字段数量

        返回哈希表中字段的数量。如果指定的 key 不存在，返回值为 0。
        """
        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                return 0

            self._check_hash_type(name)

            return len(hash_)

    @collect_command_to_aof
    def hset(
            self,
            name: str,
            key: Optional[str] = None,
            value: Optional[StringableT] = None,
            mapping: Optional[dict] = None,
            items: Optional[list] = None,
    ) -> int:
        """
        将一个或多个字段及其值设置到哈希表中。如果哈希表不存在，则会创建一个新的哈希表。

        返回值为设置成功的字段数量。
        如果字段已经存在，则会更新其值，但不会增加计数。
        """
        if key is None and not mapping and not items:
            raise ValueError("没有给出要设置的键值对")
        pieces = {}
        if items:
            for k, v in items:
                pieces[k] = v
        if key is not None:
            pieces[key] = value
        if mapping:
            pieces.update(mapping)

        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                hash_ = self.data[name] = {}
                self.data_types[name] = DataType.HASH
            else:
                self._check_hash_type(name)

            num = 0
            for k, v in pieces.items():
                if k not in hash_:
                    num += 1
                hash_[k] = v

            return num

    @collect_command_to_aof
    def hsetnx(self, name: str, key: str, value: str) -> bool:
        """
        指定的字段不存在时，设置该哈希字段的值
        """

        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                hash_ = self.data[name] = {}
                self.data_types[name] = DataType.HASH
            else:
                self._check_hash_type(name)

            if key not in hash_:
                hash_[key] = value
                return True

            return False

    # def hmset(self, name: str, mapping: dict) -> str:
    #     """
    #     从 Redis 4.0 开始，HMSET 被标记为过时，建议使用 HSET 命令来替代。
    #     """

    def hmget(self, name: str, keys: List) -> List:
        """
        从 Redis 哈希中获取一个或多个字段的值。

        返回一个数组，包含请求的字段的值。如果某个字段不存在，则返回 None。
        如果哈希不存在，返回一个空数组。
        """
        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                return []

            self._check_hash_type(name)

            return [hash_.get(k, None) for k in keys]

    def hvals(self, name: str) -> List:
        """
        获取哈希表中所有字段的值。

        如果指定的 key 不存在，返回空数组。
        """
        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                return []

            self._check_hash_type(name)

            return hash_.values()

    def hstrlen(self, name: str, key: str) -> int:
        """
        获取哈希表中指定字段的字符串长度

        如果字段不存在，返回 0。
        如果哈希表不存在，返回 0。
        """
        with self.db_lock:
            hash_ = self.data.get(name, None)
            if hash_ is None:
                return 0

            self._check_hash_type(name)

            if key not in hash_:
                return 0

            return len(hash_[key])


class Litedis(
    HashType,
    SortedSetType,
    SetType,
    ListType,
    BasicKey,
    metaclass=_SingletonMeta
):
    """模仿 Redis 接口的类"""

    def __init__(self,
                 connection_string: Optional[str] = None,
                 db_name: str = "litedis",
                 data_dir: str = "./data",
                 persistence=PersistenceType.MIXED,
                 aof_fsync=AOFFsyncStrategy.ALWAYS,
                 rdb_save_frequency: int = 600,
                 compression: bool = True,
                 singleton=True):
        """初始化数据库

        Args:
            connection_string: 数据库连接字符串，形式如: 'litedis:///path/db_name'(注意冒号后有三个连续'/')
            db_name: 数据库名称
            data_dir: 数据目录
            persistence: 持久化类型
            aof_fsync: AOF同步策略
            rdb_save_frequency: RDB保存频率(秒)
            compression: 是否压缩RDB文件
            singleton: 是否创建单例，默认是，为 False 时否
        """
        self.data: Dict[str, Any] = {}
        self.data_types: Dict[str, str] = {}
        self.expires: Dict[str, float] = {}
        self.db_lock = threading.Lock()
        self.singleton = singleton

        # 数据目录 相关
        if connection_string:
            # litedis:///path/to/db_name --> (./path/to, db_name)
            result = parse.urlparse(connection_string)
            if result.netloc:
                raise ValueError("connection_string格式不正确，应为：'litedis:///path/to/db_name'")
            path, name = result.path.replace("/", "./", 1).rsplit("/", maxsplit=1)
            self.data_dir = Path(path)
            self.db_name = name
        else:
            self.data_dir = Path(data_dir)
            self.db_name = db_name
            self.connection_string = f"litedis:///{data_dir.lstrip('./|/').rstrip('/')}/{db_name}"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # 持久化 相关
        self.persistence = persistence
        weak_self = weakref.ref(self)
        # AOF 相关
        self.aof = AOF(db=weak_self,
                       aof_fsync=aof_fsync)
        # RDB 相关
        self.rdb = RDB(db=weak_self,
                       rdb_save_frequency=rdb_save_frequency,
                       compression=compression,
                       callback_after_save_rdb=self.aof.clear_aof)
        # 过期 相关
        self.expiry = Expiry(db=weak_self)

        # 是否关闭状态
        self.closed = False

        # 加载数据
        # 尝试从 RDB 加载
        self.rdb.read_rdb()
        # 如果有 AOF , 加载到数据库, 再清理 AOF
        result = self.aof.read_aof()
        if result:
            self.rdb.save_rdb()

    def close(self):
        """
        关闭数据库
        """
        # 确保 aof 有持久化就可以了，这里的内容在重新初始化数据库的时候，会同步到 rdb 里
        # 虽然这里也可以直接保存 rdb，但rdb 可能比较费时，退出的时候，可能来不及保存好（通过 __del__触发的时候）
        self.aof.flush_buffer()
        self.closed = True

        del self

    def __del__(self):
        if not self.closed:
            self.close()

    # with 相关接口
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f"发生异常: {exc_type}, 值: {exc_val}")
        self.close()
        return True
