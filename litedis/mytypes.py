"""
定义一些类型
"""


class DataType:
    """数据库里值的类型"""
    STRING = "string"
    LIST = "list"
    HASH = "hash"
    SET = "set"


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
