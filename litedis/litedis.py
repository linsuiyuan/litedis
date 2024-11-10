import functools
import time
import threading
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Set)
from pathlib import Path

from litedis import BaseLitedis, DataType, PersistenceType, AOFFsyncStrategy
from litedis.aof import AOF
from litedis.rdb import RDB
from litedis.expiry import Expiry


class Litedis(BaseLitedis):
    """模仿 Redis 接口的类"""

    def __init__(self,
                 db_name: str = "litedb",
                 data_dir: str = "./data",
                 persistence: str = PersistenceType.AOF,
                 aof_fsync=AOFFsyncStrategy.ALWAYS,
                 rdb_save_frequency: int = 900,
                 compression: bool = True):
        """初始化数据库

        Args:
            db_name: 数据库名称
            data_dir: 数据目录
            persistence: 持久化类型
            aof_fsync: AOF同步策略
            rdb_save_frequency: RDB保存频率(秒)
            compression: 是否压缩RDB文件
        """
        self.data: Dict[str, Any] = {}
        self.data_types: Dict[str, str] = {}
        self.expires: Dict[str, float] = {}
        self.db_lock = threading.Lock()

        # 持久化相关配置
        self.data_dir = Path(data_dir)
        self.db_name = db_name
        self.persistence = persistence

        # 创建数据目录
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # RDB 相关
        self.rdb = RDB(db=self,
                       rdb_save_frequency=rdb_save_frequency,
                       compression=compression)

        # AOF 相关
        self.aof = AOF(db=self,
                       aof_fsync=aof_fsync)

        self.expiry = Expiry(expires=self.expires)

        # 加载数据
        self._load_data()

        # 启动后台任务
        self._start_background_tasks()

    @property
    def db_data(self):
        return {
            'data': self.data,
            'types': self.data_types,
            'expires': self.expires
        }

    @db_data.setter
    def db_data(self, value):
        self.data = value['data']
        self.data_types = value['types']
        self.expires = value['expires']

    def _start_background_tasks(self):
        """启动后台任务"""
        # 过期键清理线程
        self.expiry.run_handle_expired_keys_task(callback=self.delete)

        # AOF同步线程
        if self.persistence in (PersistenceType.AOF, PersistenceType.MIXED):
            self.aof.run_fsync_task_in_background()

        # RDB保存线程
        if self.persistence in (PersistenceType.RDB, PersistenceType.MIXED):
            self.rdb.save_task_in_background()

    def _load_data(self):
        """加载数据"""
        # 尝试从RDB加载
        data = self.rdb.read_rdb()
        if data:
            self.db_data = data

        # 应用AOF,
        self._apply_aof_command()

    def _apply_aof_command(self):
        """应用命令"""
        # 初始化时的不需要记录AOF
        persistence = self.persistence
        self.persistence = PersistenceType.NONE

        for command in self.aof.read_aof():
            method, args, kwargs = command.values()
            getattr(self, method)(*args, **kwargs)

        self.persistence = persistence

    @staticmethod
    def append_to_aof(func):
        """需要记录 aof 的方法加上这个装饰器就好了"""

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            result = func(self, *args, **kwargs)
            if self.persistence in (PersistenceType.AOF, PersistenceType.MIXED):
                command = {'method': func.__name__, 'args': args, "kwargs": kwargs}
                self.aof.append(command)
            return result

        return wrapper

    # 通用操作
    @append_to_aof
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
        if self.expiry.check_expired(key, callback=self.delete):
            return False
        return key in self.data

    @append_to_aof
    def expire(self, key: str, seconds: int) -> bool:
        """设置键的过期时间"""
        if key not in self.data:
            return False

        with self.db_lock:
            self.expires[key] = time.time() + seconds
            return True

    # 字符串操作
    @append_to_aof
    def set(self, key: str, value: str, ex: Optional[float] = None) -> bool:
        """设置字符串值"""
        with self.db_lock:
            self.data[key] = value
            self.data_types[key] = DataType.STRING
            if ex is not None:
                self.expires[key] = ex

        return True

    def get(self, key: str) -> Optional[str]:
        """获取字符串值"""
        if not self.exists(key):
            return None

        return self.data[key]

    # List 操作
    @append_to_aof
    def lpush(self, key: str, *values: str) -> int:
        """向列表左端推入元素"""
        with self.db_lock:
            if key not in self.data:
                self.data[key] = []
                self.data_types[key] = DataType.LIST

            if self.data_types[key] != DataType.LIST:
                raise TypeError(f"键 {key} 不是 列表")

            rev_values = list(values)
            rev_values.reverse()
            self.data[key] = rev_values + self.data[key]
            return len(self.data[key])

    @append_to_aof
    def rpush(self, key: str, *values: str) -> int:
        """向列表右端推入元素"""
        with self.db_lock:
            if key not in self.data:
                self.data[key] = []
                self.data_types[key] = DataType.LIST

            if self.data_types[key] != DataType.LIST:
                raise TypeError(f"Key {key} is not a list")

            self.data[key].extend(values)
            return len(self.data[key])

    @append_to_aof
    def lpop(self, key: str) -> Optional[str]:
        """从列表左端弹出元素"""
        if not self.exists(key):
            return None

        with self.db_lock:
            if self.data_types[key] == DataType.LIST:
                if not self.data[key]:
                    return None

                return self.data[key].pop(0)
        return None

    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """获取列表片段"""
        if not self.exists(key):
            return []

        if self.data_types[key] == DataType.LIST:
            values = self.data[key]
            # 兼容 redis 的取法
            if stop == -1:
                values = values[start:]
            elif stop < -1:
                values = values[start:stop + 1]
            else:
                values = values[start:stop]
            return values
        return []

    # Hash 操作
    @append_to_aof
    def hset(self, key: str, field: str, value: str) -> int:
        """设置哈希表字段"""
        with self.db_lock:
            if key not in self.data:
                self.data[key] = {}
                self.data_types[key] = DataType.HASH

            if self.data_types[key] != DataType.HASH:
                raise TypeError(f"Key {key} is not a hash")

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

        if self.data_types[key] == DataType.HASH:
            return dict(self.data[key])
        return {}

    # Set 操作
    @append_to_aof
    def sadd(self, key: str, *members: str) -> int:
        """添加集合成员"""
        with self.db_lock:
            if key not in self.data:
                self.data[key] = set()
                self.data_types[key] = DataType.SET

            if self.data_types[key] != DataType.SET:
                raise TypeError(f"Key {key} is not a set")

            original_size = len(self.data[key])
            self.data[key].update(members)
            return len(self.data[key]) - original_size

    def smembers(self, key: str) -> Set[str]:
        """获取集合所有成员"""
        if not self.exists(key):
            return set()

        if self.data_types[key] == DataType.SET:
            return set(self.data[key])
        return set()

    def sismember(self, key: str, member: str) -> bool:
        """判断成员是否在集合中"""
        if not self.exists(key):
            return False

        if self.data_types[key] == DataType.SET:
            return member in self.data[key]
        return False
