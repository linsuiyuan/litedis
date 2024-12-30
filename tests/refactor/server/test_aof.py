import time

import pytest

from refactor.server import LitedisServer, AOF, LitedisDb
from refactor.server.commands import create_command_from_strcmd


class TestAOF:
    @pytest.fixture(autouse=True)
    def setup_method(self, request, tmp_path):
        self.aof = AOF(tmp_path)
        self.db = LitedisDb("dbname")

    def test_append_command(self):
        cmd = create_command_from_strcmd(self.db, "get key")
        self.aof.append_command(cmd)
        assert self.aof.buffer.get() == f"{self.db.dbname}/get key"

    def test_start_and_stop(self):
        self.aof.start()
        cmd = create_command_from_strcmd(self.db, "get key")
        self.aof.append_command(cmd)
        self.aof.stop()
        assert self.aof.buffer.empty() is True