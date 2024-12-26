from typing import NamedTuple, Dict, Optional, List, Set

from refactor.typing import KeyT, LitedisObjectT, StringLikeT


class LitedisObject(NamedTuple):
    value: LitedisObjectT

    @property
    def type(self):
        if isinstance(self.value, StringLikeT):
            return "string"
        elif isinstance(self.value, List):
            return "list"
        elif isinstance(self.value, Dict):
            return "dict"
        elif isinstance(self.value, Set):
            return "set"
        else:
            raise TypeError("不支持的类型")


class LitedisDb:
    def __init__(self):
        self._data: Dict[KeyT, LitedisObject] = {}
        self._expirations: Dict[KeyT, int] = {}

    def set(self, key: KeyT, value: LitedisObject):
        self._check_value_type_consistency(key, value)
        self._data[key] = value

    def _check_value_type_consistency(self, key: KeyT, value: LitedisObject):
        if key in self._data:
            if self._data[key].type != value.type:
                raise TypeError("值类型和目标值类型不一致")

    def get(self, key: KeyT) -> Optional[LitedisObject]:
        return self._data.get(key)

    def exists(self, item: KeyT) -> bool:
        return item in self._data

    __contains__ = exists

    def delete(self, key: KeyT) -> int:
        if key not in self._data:
            return 0
        del self._data[key]
        return 1

    def keys(self):
        for key in self._data.keys():
            yield key

    def values(self):
        for value in self._data.values():
            yield value

    def set_expiration(self, key: KeyT, expiration: int) -> int:
        if key not in self._data:
            return 0
        self._expirations[key] = expiration
        return 1

    def get_expiration(self, key: KeyT):
        return self._expirations.get(key)

    def get_expirations(self) -> Dict[KeyT, int]:
        return self._expirations
