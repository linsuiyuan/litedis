from refactor.client.commands import BasicCmds
from refactor.server import LitedisDb


class TestBasicKeyCmds:
    def setup_method(self):
        self.client = BasicCmds()

        db = LitedisDb("path/to")
        db.set("key", "value")

        self.client.db = db

    def test_append(self):
        result = self.client.append("key", "1")
        assert result == 6

    def test_append_key_not_exists(self):
        result = self.client.append("not exists", "1")
        assert result == 1

    def test_copy(self):
        result = self.client.copy("key", "key1")
        assert result is True