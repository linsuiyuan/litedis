from typing import Literal

KeyT = str | int
StringLikeT = str | int
LitedisObjectT = StringLikeT | list | set | dict


PersistenceType = Literal["none", "aof", "ldb", "mixed"]
