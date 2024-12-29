import time

import pytest

from refactor.server import LitedisServer


class TestAOF:
    @pytest.fixture(autouse=True)
    def setup_method(self, request, tmp_path):
        self.server = LitedisServer(data_path=tmp_path)
        self.aof = self.server.aof

    def test_append_command(self):
        aof = self.aof
        aof.append_command("db", "get key")
        assert aof.buffer.get() == "db/get key"

    def test_start_persistence_thread(self):
        aof = self.aof
        if not aof.is_persistence_thread_running is True:
            aof.start_persistence_thread()
        aof.append_command("db", "get key")
        time.sleep(.05)
        assert aof.buffer.empty() is True

    def test_stop_persistence_thread(self):
        aof = self.aof
        if not aof.is_persistence_thread_running is True:
            aof.start_persistence_thread()
        aof.stop_persistence_thread()
        time.sleep(.05)
        assert aof.is_persistence_thread_running is False