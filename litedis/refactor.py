import json
import random
import time
from fnmatch import fnmatchcase
from typing import Optional, List, Union, Iterable, Mapping

from litedis import BaseLitedis, DataType
from litedis.typing import StringableT
from litedis.utils import list_or_args


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

    def append(self, key: str, value: StringableT) -> int:
        """
        将一个字符串值追加到已存在的键的末尾

        返回追加后的字符串的长度
        """
        with self.db_lock:
            self._set_string_value(key,
                                   str(self.data[key]) + str(value))

            return len(self.data[key])

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
            if not self.exists(source):
                return False

            if destination in self.data and not replace:
                return False

            self._set_string_value(destination,
                                   self.data[source],
                                   exat=self.expires.get(source, None))

            return True

    def decrby(self, name: str, amount: int = 1) -> int:
        """
        将指定键的整数值减少指定的增量。

        如果键不存在，DECRBY 命令会将键的值初始化为 0，然后再进行递减操作

        返回递减操作后，键的当前值
        """
        return self.incrby(name, -amount)

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
        return len(set(names) & set(self.data.keys()))

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
            if not self.exists(name):
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

    def incrby(self, name: str, amount: int = 1) -> int:
        """
        将“key”的值增加“amount”。

        如果键不存在，值将被初始化为“amount”。
        """
        with self.db_lock:
            if name not in self.data:
                self._set_string_value(name, 0, check_string_type=False)
            else:
                self._check_string_type(name)

            self.data[name] = int(self.data[name]) + amount
            return self.data[name]

    incr = incrby

    def incrbyfloat(self, name: str, amount: float = 1.0) -> float:
        """
        将键“name”的值按浮点数“amount”增加。

        如果键不存在，值将被初始化为“amount”。
        """
        with self.db_lock:

            if name not in self.data:
                self._set_string_value(name, .0, check_string_type=False)
            else:
                self._check_string_type(name)

            self.data[name] = float(self.data[name]) + amount
            return self.data[name]

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

    def mset(self, mapping: Mapping[str, StringableT]) -> bool:
        """
        根据映射设置键/值。映射是键/值对的字典。

        如果键已存在，将被覆盖，不同数据类型的也一样
        """
        with self.db_lock:
            for k, v in mapping.items():
                self._set_string_value(k, v, check_string_type=False)

        return True

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
