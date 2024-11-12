import inspect
import random
import time
import threading
import weakref
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Set, Tuple)
from pathlib import Path
from urllib import parse

from litedis import (BaseLitedis,
                     DataType,
                     PersistenceType,
                     AOFFsyncStrategy)
from litedis.aof import AOF, collect_command_to_aof
from litedis.rdb import RDB
from litedis.expiry import Expiry


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


class Litedis(BaseLitedis, metaclass=_SingletonMeta):
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

    # 其他 操作
    @collect_command_to_aof
    def delete(self, *keys: str) -> int:
        """删除键"""
        count = 0
        with self.db_lock:
            for key in keys:
                if key not in self.data:
                    continue
                self.data.pop(key, None)
                self.data_types.pop(key, None)
                self.expires.pop(key, None)
                count += 1
        return count

    def exists(self, key: str) -> bool:
        """检查键是否存在"""
        if self.expiry.check_expired(key):
            return False
        return key in self.data

    @collect_command_to_aof
    def expire(self, key: str, seconds: int) -> bool:
        """设置键的过期时间"""
        if key not in self.data:
            return False

        with self.db_lock:
            self.expires[key] = time.time() + seconds
            return True

    # 字符串 操作
    @collect_command_to_aof
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """设置字符串值"""

        if not isinstance(value, str):
            raise TypeError(f"值必须为字符串")

        with self.db_lock:
            self.data[key] = value
            self.data_types[key] = DataType.STRING
            if ex is not None:
                self.expires[key] = time.time() + ex

        return True

    def get(self, key: str) -> Optional[str]:
        """获取字符串值"""
        if not self.exists(key):
            return None

        if self.data_types[key] != DataType.STRING:
            raise TypeError(f"{key}-{self.data_types[key]} 不是字符串")

        return self.data[key]

    # 列表 操作
    @collect_command_to_aof
    def lpush(self, key: str, *values: str) -> int:
        """向列表左端推入元素"""
        with self.db_lock:
            if key not in self.data:
                self.data[key] = []
                self.data_types[key] = DataType.LIST

            if self.data_types[key] != DataType.LIST:
                raise TypeError(f"{key}-{self.data_types[key]} 不是列表")

            rev_values = list(values)
            rev_values.reverse()
            self.data[key] = rev_values + self.data[key]
            return len(self.data[key])

    @collect_command_to_aof
    def rpush(self, key: str, *values: str) -> int:
        """向列表右端推入元素"""
        with self.db_lock:
            if key not in self.data:
                self.data[key] = []
                self.data_types[key] = DataType.LIST

            if self.data_types[key] != DataType.LIST:
                raise TypeError(f"{key}-{self.data_types[key]} 不是列表类型")

            self.data[key].extend(values)
            return len(self.data[key])

    @collect_command_to_aof
    def lpop(self, key: str) -> Optional[str]:
        """从列表左端弹出元素"""
        if not self.exists(key):
            return None

        with self.db_lock:
            if not self.data[key]:
                return None

            if self.data_types[key] != DataType.LIST:
                raise TypeError(f"{key}-{self.data_types[key]} 不是列表类型")

            return self.data[key].pop(0)

    @collect_command_to_aof
    def rpop(self, key: str) -> Optional[str]:
        """从列表右端弹出元素"""
        if not self.exists(key):
            return None

        with self.db_lock:
            if not self.data[key]:
                return None

            if self.data_types[key] != DataType.LIST:
                raise TypeError(f"{key}-{self.data_types[key]} 不是列表类型")

            return self.data[key].pop()

    def llen(self, key: str) -> int:
        """获取列表长度"""
        if not self.exists(key):
            return 0
        if self.data_types[key] != DataType.LIST:
            raise TypeError(f"{key}-{self.data_types[key]} 不是列表")
        return len(self.data[key])

    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """获取列表片段"""
        if not self.exists(key):
            return []

        if not self.data[key]:
            return []

        if self.data_types[key] != DataType.LIST:
            raise TypeError(f"{key}-{self.data_types[key]} 不是列表类型")

        values = self.data[key]
        # 处理索引, Redis 是包含右边界的
        if stop < 0:
            stop = len(values) + stop + 1
        else:
            stop += 1
        return values[start:stop]

    # 哈希 操作
    @collect_command_to_aof
    def hset(self, key: str, field: str, value: str) -> int:
        """设置哈希表字段"""
        with self.db_lock:
            if key not in self.data:
                self.data[key] = {}
                self.data_types[key] = DataType.HASH

            if self.data_types[key] != DataType.HASH:
                raise TypeError(f"{key}-{self.data_types[key]} 不是哈希类型")

            is_new = field not in self.data[key]
            self.data[key][field] = value
            return 1 if is_new else 0

    def hget(self, key: str, field: str) -> Optional[str]:
        """获取哈希表字段"""
        if not self.exists(key):
            return None

        if self.data_types[key] == DataType.HASH:
            return self.data[key].get(field)
        return None

    def hgetall(self, key: str) -> Dict[str, str]:
        """获取所有哈希表字段"""
        if not self.exists(key):
            return {}

        if self.data_types[key] != DataType.HASH:
            raise TypeError(f"{key}-{self.data_types[key]} 不是哈希类型")

        return dict(self.data[key])

    # 集合 操作
    @collect_command_to_aof
    def sadd(self, key: str, *members: str) -> int:
        """添加集合成员"""
        with self.db_lock:
            if key not in self.data:
                self.data[key] = set()
                self.data_types[key] = DataType.SET

            if self.data_types[key] != DataType.SET:
                raise TypeError(f"{key}-{self.data_types[key]} 不是集合类型")

            original_size = len(self.data[key])
            self.data[key].update(members)
            return len(self.data[key]) - original_size

    def smembers(self, key: str) -> Set[str]:
        """获取集合所有成员"""
        if not self.exists(key):
            return set()

        if self.data_types[key] != DataType.SET:
            raise TypeError(f"{key}-{self.data_types[key]} 不是集合类型")

        return set(self.data[key])

    def sismember(self, key: str, member: str) -> bool:
        """判断成员是否在集合中"""
        if not self.exists(key):
            return False

        if self.data_types[key] != DataType.SET:
            raise TypeError(f"{key}-{self.data_types[key]} 不是集合类型")

        return member in self.data[key]

    # 有序集合 操作
    @collect_command_to_aof
    def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        """
        添加有序集合成员

        不存在就添加，存在则更新分数

        用法: zadd(key, {"member1": score1, "member2": score2, ...})
        """
        with self.db_lock:
            if key not in self.data:
                self.data[key] = {}
                self.data_types[key] = DataType.ZSET

            if self.data_types[key] != DataType.ZSET:
                raise TypeError(f"{self.data_types[key]} 不是有序集合")

            count = 0
            for member, score in mapping.items():
                score = float(score)
                if member not in self.data[key] or self.data[key][member] != score:
                    self.data[key][member] = score
                    count += 1

            return count

    def zcard(self, key: str) -> int:
        """获取有序集合的成员数量"""
        if not self.exists(key):
            return 0

        if self.data_types[key] != DataType.ZSET:
            raise TypeError(f"{self.data_types[key]} 不是有序集合")

        return len(self.data[key])

    @collect_command_to_aof
    def zincrby(self, key: str, increment: float, member: str) -> float:
        """增加有序集合成员的分数"""
        if not self.exists(key):
            self.data[key] = {}
            self.data_types[key] = DataType.ZSET

        if self.data_types[key] != DataType.ZSET:
            raise TypeError(f"{self.data_types[key]} 不是有序集合")

        with self.db_lock:
            current_score = self.data[key].get(member, 0.0)
            new_score = current_score + increment
            self.data[key][member] = new_score
            return new_score

    def zrandmember(self, key: str, count: int = 1) -> List[str]:
        """随机获取有序集合的成员"""
        if not self.exists(key):
            return []

        if self.data_types[key] != DataType.ZSET:
            raise TypeError(f"{self.data_types[key]} 不是有序集合")

        members = list(self.data[key].keys())
        if count < 0:
            count = abs(count)
        if count > len(members):
            count = len(members)
        return random.sample(members, count)

    def zrange(
            self,
            key: str,
            start: int,
            stop: int,
            withscores: bool = False
    ) -> List[Any]:
        """获取有序集合的范围"""
        if not self.exists(key):
            return []

        if self.data_types[key] != DataType.ZSET:
            raise TypeError(f"{self.data_types[key]} 不是有序集合")

        # 按分数排序
        sorted_members = sorted(self.data[key].items(), key=lambda x: (x[1], x[0]))

        # 处理索引, Redis 是包含右边界的
        if stop < 0:
            stop = len(sorted_members) + stop + 1
        else:
            stop += 1

        result = sorted_members[start:stop]

        if withscores:
            return [(member, score) for member, score in result]
        return [member for member, _ in result]

    def zrangebyscore(self, key: str, min_score: float, max_score: float) -> List[str]:
        """根据分数范围获取有序集合的成员"""
        if not self.exists(key):
            return []

        if self.data_types[key] != DataType.ZSET:
            raise TypeError(f"{self.data_types[key]} 不是有序集合")

        sorted_members = sorted(self.data[key].items(), key=lambda x: (x[1], x[0]))

        return [member
                for member, score in sorted_members
                if min_score <= score <= max_score]

    @collect_command_to_aof
    def zrem(self, key: str, *members: str) -> int:
        """移除有序集合中的成员"""
        if not self.exists(key):
            return 0

        if self.data_types[key] != DataType.ZSET:
            raise TypeError(f"{self.data_types[key]} 不是有序集合")

        count = 0
        with self.db_lock:
            for member in members:
                if member in self.data[key]:
                    del self.data[key][member]
                    count += 1
        return count

    def zscan(self,
              key: str,
              cursor: int = 0,
              count: int = 10) -> Tuple[int, List[str]]:
        """
        扫描有序集合成员

        redis 里 scan 是非阻塞的，所以这里也是，因此结果可能有重复或遗漏
        """
        if not self.exists(key):
            return 0, []

        if self.data_types[key] != DataType.ZSET:
            raise TypeError(f"{self.data_types[key]} 不是有序集合")

        members = sorted(self.data[key].items(), key=lambda x: (x[1], x[0]))
        total = len(members)
        start = cursor
        end = min(cursor + count, total)
        next_cursor = end if end < total else 0
        return next_cursor, members[start:end]

    def zscore(self, key: str, member: str) -> Optional[float]:
        """获取有序集合成员的分数"""
        if not self.exists(key):
            return None

        if self.data_types[key] != DataType.ZSET:
            raise TypeError(f"{self.data_types[key]} 不是有序集合")

        return self.data[key].get(member)
