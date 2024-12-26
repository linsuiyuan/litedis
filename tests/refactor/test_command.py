import time

import pytest

from refactor.command import SetCommand, GetCommand
from refactor.db import LitedisDb
from refactor.utils import parse_string_command

class TestSetCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_setcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return SetCommand(db=self.db,
                          name=name,
                          args=args)

    def test_execute_with_key_value(self):
        strcmd = "set key1 value1"
        command = self._create_setcommand_from_strcmd(strcmd)
        command.execute()
        assert self.db.get("key1") == "value1"

    @pytest.mark.parametrize("strcmd, expected", [
        ("set key1 value1 nx", None),
        ("set key1 value1 xx", "OK"),
    ])
    def test_execute_with_nx_xx(self, strcmd, expected):
        command = self._create_setcommand_from_strcmd("set key1 value1")
        command.execute()
        command = self._create_setcommand_from_strcmd(strcmd)
        res = command.execute()
        assert res == expected

    @pytest.mark.parametrize("strcmd", [
        "set key1 value1 ex 60",
        "set key1 value1 px 60000",
        f"set key1 value1 exat {int(time.time()) + 60}",
        f"set key1 value1 pxat {int(time.time() * 1000) + 60*1000}"
    ])
    def test_execute_with_expiration(self, strcmd):
        command = self._create_setcommand_from_strcmd(strcmd)
        command.execute()
        expiration = self.db.get_expiration("key1")
        assert expiration // 1000 == int(time.time()) + 60

class TestGetCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        # set a key-value for testing
        set_command = SetCommand(db=self.db, name="set", args=["key1", "value1"])
        set_command.execute()

    def _create_getcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return GetCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key(self):
        strcmd = "get key1"
        command = self._create_getcommand_from_strcmd(strcmd)
        result = command.execute()
        assert result == "value1"

    def test_execute_with_non_existing_key(self):
        strcmd = "get key2"
        command = self._create_getcommand_from_strcmd(strcmd)
        result = command.execute()
        assert result is None
