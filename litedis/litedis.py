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
                 db_name: str = "litedis",
                 data_dir: str = "./data",
                 persistence: str = PersistenceType.MIXED,
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

    def _start_background_tasks(self):
        """启动后台任务"""
        # 过期键清理线程
        self.expiry.run_handle_expired_keys_task(callback=self.delete)

        # AOF同步线程
        if self.persistence in (PersistenceType.AOF, PersistenceType.MIXED):
            self.aof.run_fsync_task_in_background()

        # RDB保存线程
        if self.persistence in (PersistenceType.RDB, PersistenceType.MIXED):
            self.rdb.save_task_in_background(callback=self.aof.clear_aof)

    def _load_data(self):
        """加载数据"""
        # 尝试从RDB加载
        self.rdb.read_rdb()

        # 应用AOF
        self._apply_aof_command()

        # 如果有读取 AOF , 保存数据库, 再清理 AOF
        if self.aof.aof_path.exists():
            self.rdb.save_rdb(callback=self.aof.clear_aof)

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
                raise TypeError(f"{key}-{self.data_types[key]} 不是 列表")

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
                raise TypeError(f"{key}-{self.data_types[key]} 不是列表类型")

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
            # 处理索引, Redis 是包含右边界的
            if stop < 0:
                stop = len(values) + stop + 1
            else:
                stop += 1
            return values[start:stop]
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
                raise TypeError(f"{key}-{self.data_types[key]} 不是集合类型")

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

    @append_to_aof
    def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        """添加有序集合成员

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

    def zscore(self, key: str, member: str) -> Optional[float]:
        """获取有序集合成员的分数"""
        if not self.exists(key):
            return None

        if self.data_types[key] != DataType.ZSET:
            raise TypeError(f"{self.data_types[key]} 不是有序集合")

        return self.data[key].get(member)

    def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> List[Any]:
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
