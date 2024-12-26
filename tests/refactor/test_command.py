import time

import pytest

from refactor.command import SetCommand
from refactor.server import LitedisServer
from refactor.utils import parse_string_command

class TestSetCommand:
    def setup_method(self):
        self.db = LitedisServer().get_or_create_db("path/to")

    def _parse_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return SetCommand(db=self.db,
                          name=name,
                          args=args)

    def test_set_key_value(self):
        strcmd = "set key1 value1"
        command = self._parse_strcmd(strcmd)
        assert command.key == "key1"
        assert command.value.value == "value1"

    @pytest.mark.parametrize("strcmd, expected", [
        ("set key1 value1 nx", (True, False)),
        ("set key1 value1 xx", (False, True)),
    ])
    def test_nx_xx_property(self, strcmd, expected):
        command = self._parse_strcmd(strcmd)
        assert (command.nx, command.xx) == expected

    @pytest.mark.parametrize("strcmd", [
        "set key1 value1 ex 60",
        "set key1 value1 px 60000",
        f"set key1 value1 exat {int(time.time()) + 60}",
        f"set key1 value1 pxat {int(time.time() * 1000) + 60*1000}"
    ])
    def test_expiration_property(self, strcmd):
        command = self._parse_strcmd(strcmd)
        assert command.expiration // 1000 == int(time.time()) + 60

    def test_execute(self):
        strcmd = "set key1 value1 ex 60"
        command = self._parse_strcmd(strcmd)
        command.execute()
        assert self.db.get("key1").value == "value1"
