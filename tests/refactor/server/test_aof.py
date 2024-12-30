import pytest

from refactor.server import AOF, LitedisDb


class TestAOF:
    @pytest.fixture(autouse=True)
    def setup_method(self, request, tmp_path):
        self.aof = AOF(tmp_path)
        self.db = LitedisDb("dbname")