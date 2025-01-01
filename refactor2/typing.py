from enum import Enum

LitedisObjectT = dict | int | list | set | str

class PersistenceType(Enum):
    NONE = "none"
    AOF = "aof"
    LDB = "ldb"
    MIXED = "mixed"