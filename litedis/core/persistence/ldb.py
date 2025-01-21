import time
from typing import Dict, Optional

from litedis.core.command.sortedset import SortedSet
from litedis.typing import LitedisObjectT


class LitedisDB:
    def __init__(self, name):
        self.name = name
        self._data: Dict[str, LitedisObjectT] = {}
        self._expirations: Dict[str, int] = {}

    def set(self, key: str, value: LitedisObjectT):
        self._check_value_type(key, value)
        self._data[key] = value

    def _check_value_type(self, key: str, value: LitedisObjectT):
        if not type(value) in [str, list, dict, set, SortedSet]:
            raise TypeError(f"not supported type {type(value)}")
        if key in self._data:
            if type(self._data[key]) != type(value):
                raise TypeError("type of value does not match the type in database")

    def get(self, key: str) -> Optional[LitedisObjectT]:
        if self._delete_expired(key):
            return None
        return self._data.get(key)

    def get_str(self, key: str) -> Optional[str]:
        value = self.get(key)
        if value is None:
            return None
        if type(value) != str:
            raise TypeError("value is not string")
        return value

    def get_dict(self, key: str) -> Optional[dict]:
        value = self.get(key)
        if value is None:
            return None
        if type(value) != dict:
            raise TypeError("value is not a hash")
        return value

    def get_list(self, key: str) -> Optional[list]:
        value = self.get(key)
        if value is None:
            return None
        if type(value) != list:
            raise TypeError("value is not a list")
        return value

    def get_set(self, key: str) -> Optional[set]:
        value = self.get(key)
        if value is None:
            return None
        if type(value) != set:
            raise TypeError("value is not a set")
        return value

    def get_zset(self, key: str) -> Optional[SortedSet]:
        value = self.get(key)
        if value is None:
            return None
        if type(value) != SortedSet:
            raise TypeError("value is not a zset")
        return value

    def _delete_expired(self, key: str):
        if key not in self._data:
            return False

        if key not in self._expirations:
            return False

        expiration = self._expirations[key]
        if expiration >= int(time.time() * 1000):
            return False

        del self._data[key]
        del self._expirations[key]
        return True

    def exists(self, item: str) -> bool:
        if self._delete_expired(item):
            return False
        return item in self._data

    def delete(self, key: str) -> int:
        if key not in self._data:
            return 0
        del self._data[key]
        self.delete_expiration(key)
        return 1

    def keys(self):
        for key in self._data.keys():
            yield key

    def set_expiration(self, key: str, expiration: int) -> int:
        if key not in self._data:
            return 0
        self._expirations[key] = expiration
        return 1

    def get_expiration(self, key: str) -> int:
        if key not in self._data:
            return -2
        if key not in self._expirations:
            return -1
        return self._expirations.get(key)

    def exists_expiration(self, key: str) -> bool:
        return key in self._expirations

    def delete_expiration(self, key: str) -> int:
        if key not in self._expirations:
            return 0
        del self._expirations[key]
        return 1

    def get_type(self, key: str) -> str:
        if key not in self._data:
            return "none"

        value = self._data[key]
        if isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            return "list"
        elif isinstance(value, dict):
            return "hash"
        elif isinstance(value, set):
            return "set"
        elif isinstance(value, SortedSet):
            return "zset"
        else:
            raise TypeError(f"not supported type {type(value)}")
