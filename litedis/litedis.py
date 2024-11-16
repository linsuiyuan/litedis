import inspect
import threading
import weakref
from typing import Any, Dict, Optional
from pathlib import Path
from urllib import parse

from litedis import PersistenceType, AOFFsyncStrategy
from litedis.aof import AOF
from litedis.rdb import RDB
from litedis.expiry import Expiry
from litedis.typemixin import HashType, SortedSetType, SetType, ListType, BasicKey


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
