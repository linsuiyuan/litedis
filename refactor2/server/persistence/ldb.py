from refactor2.typing import LitedisObjectT


class LitedisDB:
    def __init__(self, name):
        self.name = name
        self._data: dict[str, LitedisObjectT] = {}
        self._expirations: dict[str, int] = {}

    def set(self, key: str, value: LitedisObjectT):
        self._check_value_type(key, value)
        self._data[key] = value

    def _check_value_type(self, key: str, value: LitedisObjectT):
        if not isinstance(value, LitedisObjectT):
            raise TypeError(f"not supported type {type(value)}")
        if key in self._data:
            if type(self._data[key]) != type(value):
                raise TypeError("type of value does not match the type in database")

    def get(self, key: str) -> LitedisObjectT | None:
        return self._data.get(key)

    def exists(self, item: str) -> bool:
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

    def values(self):
        for value in self._data.values():
            yield value

    def set_expiration(self, key: str, expiration: int) -> int:
        if key not in self._data:
            return 0
        self._expirations[key] = expiration
        return 1

    def get_expiration(self, key: str):
        return self._expirations.get(key)

    def exists_expiration(self, key: str) -> bool:
        return key in self._expirations

    def delete_expiration(self, key: str) -> int:
        if key not in self._expirations:
            return 0
        del self._expirations[key]
        return 1

    def get_expirations(self) -> dict[str, int]:
        return self._expirations

    def get_type(self, key: str) -> str | None:
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
        else:
            raise TypeError(f"not supported type {type(value)}")