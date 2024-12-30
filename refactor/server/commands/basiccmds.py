import time
from functools import lru_cache

from refactor.server.commands import Command
from refactor.typing import StringLikeT, KeyT, LitedisObjectT

class SetCommand(Command):
    name = 'set'

    def _check_args_count(self):
        if len(self.args) < 2:
            raise ValueError(f"SetCommand takes more than 1 arguments, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    @property
    def value(self) -> LitedisObjectT:
        return self.args[1]

    @property
    def nx(self) -> bool:
        lower_args = self._lower_args_omit_first_two()
        return "nx" in lower_args

    @property
    def xx(self) -> bool:
        lower_args = self._lower_args_omit_first_two()
        return "xx" in lower_args

    @property
    @lru_cache
    def expiration(self) -> int:
        lower_args = self._lower_args_omit_first_two()
        if len(lower_args) < 2:
            return 0

        if "ex" in lower_args:
            expiration = self._get_ex_milli_timestamp(lower_args)
        elif "px" in lower_args:
            expiration = self._get_px_milli_timestamp(lower_args)
        elif "exat" in lower_args:
            expiration = self._get_exat_milli_timestamp(lower_args)
        elif "pxat" in lower_args:
            expiration = self._get_pxat_milli_timestamp(lower_args)
        else:
            expiration = 0

        return expiration

    def execute(self):

        self._check_that_nx_and_xx_cannot_both_be_set()

        if self._if_nx_is_set_and_key_exists():
            return None

        if self._if_xx_is_set_and_key_not_exists():
            return None

        self.db.set(self.key, self.value)
        self.db.delete_expiration(self.key)

        self._set_expiration_if_repiration_is_set()

        return "OK"

    def _check_that_nx_and_xx_cannot_both_be_set(self):
        if self.nx and self.xx:
            raise ValueError("nx and xx cannot be set at the same time")

    def _if_nx_is_set_and_key_exists(self):
        return self.nx and self.key in self.db

    def _if_xx_is_set_and_key_not_exists(self):
        return self.xx and self.key not in self.db

    def _set_expiration_if_repiration_is_set(self):
        if self.expiration:
            self.db.set_expiration(self.key, self.expiration)

    def _lower_args_omit_first_two(self) -> list[StringLikeT]:
        return [s.lower() if isinstance(s, str) else s
                for s in self.args[2:]]

    def _get_ex_milli_timestamp(self, args) -> int:
        index = args.index("ex")
        now = int(time.time() * 1000)
        return now + args[index + 1] * 1000

    def _get_px_milli_timestamp(self, args) -> int:
        index = args.index("px")
        now = int(time.time() * 1000)
        return now + args[index + 1]

    def _get_exat_milli_timestamp(self, args) -> int:
        index = args.index("exat")
        return args[index + 1] * 1000

    def _get_pxat_milli_timestamp(self, args) -> int:
        index = args.index("pxat")
        return args[index + 1]


class GetCommand(Command):
    name = 'get'

    def _check_args_count(self):
        if len(self.args) < 1:
            raise ValueError(f"GetCommand requires at least 1 argument, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    def execute(self):
        return self.db.get(self.key)


class AppendCommand(Command):
    name = 'append'

    def _check_args_count(self):
        if len(self.args) != 2:
            raise ValueError(f"AppendCommand requires 2 arguments, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    @property
    def value(self) -> str:
        return self.args[1]

    def execute(self):
        self._set_default_if_key_not_exists(self.key, "")

        new_value = self._append_string_to_the_value_of_key()

        return len(new_value)

    def _append_string_to_the_value_of_key(self):
        current_value = str(self.db.get(self.key))
        new_value = current_value + str(self.value)
        self.db.set(self.key, new_value)
        return new_value


class DecrbyCommand(Command):
    name = 'decrby'

    def _check_args_count(self):
        if len(self.args) != 2:
            raise ValueError(f"DecrbyCommand requires 2 arguments, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    @property
    def decrement(self) -> str:
        return self.args[1]

    def execute(self):

        self._set_default_if_key_not_exists(self.key, "0")

        try:
            return self._decrement_the_value()
        except ValueError:
            raise ValueError("value is not an integer")

    def _decrement_the_value(self):
        current_value = int(self.db.get(self.key))
        decrement = int(self.decrement)
        new_value = current_value - decrement
        self.db.set(self.key, str(new_value))
        return new_value


class DeleteCommand(Command):
    name = 'del'

    def _check_args_count(self):
        if len(self.args) < 1:
            raise ValueError(f"DeleteCommand requires at least 1 argument, {len(self.args)} given")

    @property
    def keys(self) -> list[KeyT]:
        return self.args

    def execute(self):
        deleted_count = 0
        for key in self.keys:
            if key in self.db:
                self.db.delete(key)
                deleted_count += 1
        return deleted_count


class ExistsCommand(Command):
    name = 'exists'

    def _check_args_count(self):
        if len(self.args) < 1:
            raise ValueError(f"ExistsCommand requires at least 1 argument, {len(self.args)} given")

    @property
    def keys(self) -> list[KeyT]:
        return self.args

    def execute(self):
        count = 0
        for key in self.keys:
            if key in self.db:
                count += 1
        return count


class CopyCommand(Command):
    name = 'copy'

    def _check_args_count(self):
        if len(self.args) < 2:
            raise ValueError(f"CopyCommand requires at least 2 arguments, {len(self.args)} given")

    @property
    def source(self) -> KeyT:
        return self.args[0]

    @property
    def destination(self) -> KeyT:
        return self.args[1]

    @property
    def replace(self) -> bool:
        return len(self.args) > 2 and self.args[2].lower() == "replace"

    def execute(self):
        if self.source not in self.db:
            return 0

        if self._is_destination_exists_and_has_no_replace_arg():
            return 0

        self._copy_source_to_destination()

        self._copy_source_expiration_to_destination_if_exists()

        return 1

    def _is_destination_exists_and_has_no_replace_arg(self):
        return self.replace is False and self.destination in self.db

    def _copy_source_to_destination(self):
        value = self.db.get(self.source)
        self.db.set(self.destination, value)

    def _copy_source_expiration_to_destination_if_exists(self):
        expiration = self.db.get_expiration(self.source)
        if expiration:
            self.db.set_expiration(self.destination, expiration)


class ExpireCommand(Command):
    name = 'expire'

    def _check_args_count(self):
        if len(self.args) != 2:
            raise ValueError(f"ExpireCommand requires 2 arguments, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    @property
    def seconds(self) -> str:
        return self.args[1]

    def execute(self):
        if self.key not in self.db:
            return 0

        self._set_expiration()
        return 1

    def _set_expiration(self):
        expiration = int(time.time() * 1000) + int(self.seconds) * 1000
        self.db.set_expiration(self.key, expiration)


class ExpireatCommand(Command):
    name = 'expireat'

    def _check_args_count(self):
        if len(self.args) != 2:
            raise ValueError(f"ExpireatCommand requires 2 arguments, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    @property
    def timestamp(self) -> str:
        return self.args[1]

    def execute(self):
        if self.key not in self.db:
            return 0

        self._set_expiration()
        return 1

    def _set_expiration(self):
        expiration = int(self.timestamp) * 1000
        self.db.set_expiration(self.key, expiration)


class ExpireTimeCommand(Command):
    name = 'expiretime'

    def _check_args_count(self):
        if len(self.args) != 1:
            raise ValueError(f"ExpireTimeCommand requires 1 argument, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    def execute(self):
        if self.key not in self.db:
            return -2

        if not self.db.exists_expiration(self.key):
            return -1

        return self._get_expiration_time_in_seconds()

    def _get_expiration_time_in_seconds(self):
        expiration = self.db.get_expiration(self.key)
        return expiration // 1000


class IncrbyCommand(Command):
    name = 'incrby'

    def _check_args_count(self):
        if len(self.args) != 2:
            raise ValueError(f"IncrbyCommand requires 2 arguments, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    @property
    def increment(self) -> str:
        return self.args[1]

    def execute(self):

        self._set_default_if_key_not_exists(self.key, "0")

        try:
            return self._increment_the_value()
        except ValueError:
            raise ValueError("value is not an integer")

    def _increment_the_value(self):
        current_value = int(self.db.get(self.key))
        increment = int(self.increment)
        new_value = current_value + increment
        self.db.set(self.key, str(new_value))
        return new_value


class IncrbyfloatCommand(Command):
    name = 'incrbyfloat'

    def _check_args_count(self):
        if len(self.args) != 2:
            raise ValueError(f"IncrbyfloatCommand requires 2 arguments, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    @property
    def increment(self) -> str:
        return self.args[1]

    def execute(self):

        self._set_default_if_key_not_exists(self.key, "0")

        try:
            return self._increment_the_float_value()
        except ValueError:
            raise ValueError("value is not a valid float")

    def _increment_the_float_value(self):
        current_value = float(self.db.get(self.key))
        increment = float(self.increment)
        new_value = current_value + increment
        # Format float to remove trailing zeros
        formatted_value = f"{new_value:g}"
        self.db.set(self.key, formatted_value)
        return formatted_value


class KeysCommand(Command):
    name = 'keys'

    def _check_args_count(self):
        if len(self.args) != 1:
            raise ValueError(f"KeysCommand requires 1 argument, {len(self.args)} given")

    @property
    def pattern(self) -> str:
        return self.args[0]

    def execute(self):

        regex = self._convert_pattern_to_regex()

        return self._match_keys_by_regex(regex)

    def _convert_pattern_to_regex(self):
        pattern = self.pattern.replace("*", ".*").replace("?", ".")
        import re
        return re.compile(f"^{pattern}$")

    def _match_keys_by_regex(self, regex):
        matched_keys = []
        for key in self.db.keys():
            if regex.match(str(key)):
                matched_keys.append(key)
        return matched_keys


class MgetCommand(Command):
    name = 'mget'

    def _check_args_count(self):
        if len(self.args) < 1:
            raise ValueError(f"MgetCommand requires at least 1 argument, {len(self.args)} given")

    @property
    def keys(self) -> list[KeyT]:
        return self.args

    def execute(self):
        return [self.db.get(key) for key in self.keys]


class MsetCommand(Command):
    name = 'mset'

    def _check_args_count(self):
        if len(self.args) < 2 or len(self.args) % 2 != 0:
            raise ValueError(f"MsetCommand requires even number of arguments, {len(self.args)} given")

    @property
    def pairs(self) -> list[tuple[KeyT, str]]:
        return [(self.args[i], self.args[i + 1])
                for i in range(0, len(self.args), 2)]

    def execute(self):

        for i in range(0, len(self.args), 2):
            key = self.args[i]
            value = self.args[i + 1]
            self.db.set(key, value)
            self.db.delete_expiration(key)

        return "OK"


class MsetnxCommand(Command):
    name = 'msetnx'

    def _check_args_count(self):
        if len(self.args) < 2 or len(self.args) % 2 != 0:
            raise ValueError(f"MsetnxCommand requires even number of arguments, {len(self.args)} given")

    @property
    def pairs(self) -> list[tuple[KeyT, str]]:
        return [(self.args[i], self.args[i + 1])
                for i in range(0, len(self.args), 2)]

    def execute(self):

        if self._is_any_key_exists():
            return 0

        for key, value in self.pairs:
            self.db.set(key, value)
            self.db.delete_expiration(key)
        return 1

    def _is_any_key_exists(self):
        for key, _ in self.pairs:
            if key in self.db:
                return True
        return False


class PersistCommand(Command):
    name = 'persist'

    def _check_args_count(self):
        if len(self.args) != 1:
            raise ValueError(f"PersistCommand requires 1 argument, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    def execute(self):

        if self.key not in self.db:
            return 0

        return self.db.delete_expiration(self.key)


class RandomKeyCommand(Command):
    name = 'randomkey'

    def _check_args_count(self):
        if len(self.args) != 0:
            raise ValueError(f"RandomKeyCommand takes no arguments, {len(self.args)} given")

    def execute(self):

        import random

        keys = list(self.db.keys())
        if not keys:
            return None

        return random.choice(keys)


class RenameCommand(Command):
    name = 'rename'

    def _check_args_count(self):
        if len(self.args) != 2:
            raise ValueError(f"RenameCommand requires 2 arguments, {len(self.args)} given")

    @property
    def source(self) -> KeyT:
        return self.args[0]

    @property
    def destination(self) -> KeyT:
        return self.args[1]

    def execute(self):

        self._check_if_source_exists()

        value, expiration = self._get_source_value_and_expiration()

        self._set_destination_with_source_value_and_expiration(value, expiration)

        self.db.delete(self.source)
        return "OK"

    def _check_if_source_exists(self):
        if self.source not in self.db:
            raise KeyError("source key does not exist")

    def _get_source_value_and_expiration(self):
        value = self.db.get(self.source)
        expiration = self.db.get_expiration(self.source)
        return value, expiration

    def _set_destination_with_source_value_and_expiration(self, value, expiration):
        self.db.set(self.destination, value)
        if expiration:
            self.db.set_expiration(self.destination, expiration)


class RenamenxCommand(Command):
    name = 'renamenx'

    def _check_args_count(self):
        if len(self.args) != 2:
            raise ValueError(f"RenamenxCommand requires 2 arguments, {len(self.args)} given")

    @property
    def source(self) -> KeyT:
        return self.args[0]

    @property
    def destination(self) -> KeyT:
        return self.args[1]

    def execute(self):
        self._check_if_source_exists()

        if self._is_destination_exists():
            return 0

        value, expiration = self._get_source_value_and_expiration()

        self.set_destination_with_source_value_and_expiration(value, expiration)

        self.db.delete(self.source)
        return 1

    def set_destination_with_source_value_and_expiration(self, value, expiration):
        self.db.set(self.destination, value)
        if expiration:
            self.db.set_expiration(self.destination, expiration)

    def _get_source_value_and_expiration(self):
        value = self.db.get(self.source)
        expiration = self.db.get_expiration(self.source)
        return value, expiration

    def _check_if_source_exists(self):
        if self.source not in self.db:
            raise KeyError("source key does not exist")

    def _is_destination_exists(self):
        return self.destination in self.db


class StrlenCommand(Command):
    name = 'strlen'

    def _check_args_count(self):
        if len(self.args) != 1:
            raise ValueError(f"StrlenCommand requires 1 argument, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    def execute(self):
        if self.key not in self.db:
            return 0
        return len(str(self.db.get(self.key)))


class SubstrCommand(Command):
    name = 'substr'

    def _check_args_count(self):
        if len(self.args) != 3:
            raise ValueError(f"SubstrCommand requires 3 arguments, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    @property
    def start(self) -> str:
        return self.args[1]

    @property
    def end(self) -> str:
        return self.args[2]

    def execute(self):
        if self.key not in self.db:
            return ""

        try:
            return self._get_substring()
        except (ValueError, IndexError):
            return ""

    def _get_substring(self):
        value = str(self.db.get(self.key))
        start = int(self.start)
        end = int(self.end)
        # Handle negative indices
        if start < 0:
            start = len(value) + start
        if end < 0:
            end = len(value) + end
        # Ensure end is inclusive
        end = end + 1
        return value[start:end]


class TTLCommand(Command):
    name = 'ttl'

    def _check_args_count(self):
        if len(self.args) != 1:
            raise ValueError(f"TTLCommand requires 1 argument, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    def execute(self):

        if self.key not in self.db:
            return -2

        if not self.db.exists_expiration(self.key):
            return -1

        return self._get_expiration_seconds()

    def _get_expiration_seconds(self):
        expiration = self.db.get_expiration(self.key)
        now = int(time.time() * 1000)
        remaining = (expiration - now) // 1000
        return max(remaining, 0)  # Don't return negative TTL


class TypeCommand(Command):
    name = 'type'

    def _check_args_count(self):
        if len(self.args) != 1:
            raise ValueError(f"TypeCommand requires 1 argument, {len(self.args)} given")

    @property
    def key(self) -> KeyT:
        return self.args[0]

    def execute(self):
        return self.db.get_type(self.key)
