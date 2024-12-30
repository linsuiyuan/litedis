import pytest

from refactor.client.commands import BasicCmds
from refactor.server import LitedisDb, LitedisServer


class TestBasicKeyCmds:
    @pytest.fixture(autouse=True)
    def setup_method(self, request, tmp_path):
        self.client = BasicCmds()

        db = LitedisDb("path/to")
        db.set("key", "value")

        server = LitedisServer(tmp_path)

        self.client.db = db
        self.client.server = server

    def test_append(self):
        result = self.client.append("key", "1")
        assert result == 6

    def test_append_key_not_exists(self):
        result = self.client.append("not exists", "1")
        assert result == 1

    def test_copy(self):
        result = self.client.copy("key", "key1")
        assert result is True