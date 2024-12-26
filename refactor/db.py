from refactor.typing import KeyT, LitedisObjectT

class LitedisDb:
    def __init__(self, id_):
        self.id_ = id_
        self._data: dict[KeyT, LitedisObjectT] = {}
        self._expirations: dict[KeyT, int] = {}

    def set(self, key: KeyT, value: LitedisObjectT):
        self._check_value_type_consistency(key, value)
        self._data[key] = value

    def _check_value_type_consistency(self, key: KeyT, value: LitedisObjectT):
        if key in self._data:
            if not isinstance(value, LitedisObjectT):
                raise TypeError(f"not supported type {type(value)}")
            if type(self._data[key]) != type(value):
                raise TypeError("值类型和目标值类型不一致")

    def get(self, key: KeyT) -> LitedisObjectT | None:
        return self._data.get(key)

    def exists(self, item: KeyT) -> bool:
        return item in self._data

    def __contains__(self, item: KeyT) -> bool:
        return item in self._data

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

    def delete_expiration(self, key: KeyT) -> int:
        if key not in self._expirations:
            return 0
        del self._expirations[key]
        return 1

    def get_expirations(self) -> dict[KeyT, int]:
        return self._expirations
