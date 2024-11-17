import threading
from pathlib import Path
from typing import Dict, Any


class DataType:
    """数据库里值的类型"""
    STRING = "string"
    LIST = "list"
    HASH = "hash"
    SET = "set"
    SortedSet = "sortedset"


class PersistenceType:
    """
    持久化类型
    MIXED 表示同时使用 AOF 和 RDB
    """
    NONE = "none"
    AOF = "aof"
    RDB = "rdb"
    MIXED = "mixed"


class AOFFsyncStrategy:
    """AOF 同步策略"""
    NO = "no"
    ALWAYS = "always"
    EVERYSEC = "everysec"


class BaseLitedis:
    data: Dict[str, Any]
    data_types: Dict[str, str]
    expires: Dict[str, float]
    db_lock: threading.Lock

    db_name: str
    data_dir: Path

    persistence: PersistenceType

    def delete(self, *keys: str) -> int:
        raise NotImplementedError("子类实现")


from litedis.aof import AOF  # noqa
from litedis.expiry import Expiry  # noqa
from litedis.rdb import RDB  # noqa
from litedis.litedis import Litedis  # noqa
