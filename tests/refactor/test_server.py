from refactor.server import LitedisServer


class TestLitedisServer:
    def setup_method(self):
        self.server = LitedisServer()

    def test_get_db_is_same_db_by_id(self):
        id_ = "path/to"
        db1 = self.server.get_or_create_db(id_)
        db2 = self.server.get_or_create_db(id_)
        assert db1 is db2

    def test_get_db_is_not_same_db_by_id(self):
        id1 = "path/to/1"
        id2 = "path/to/2"
        db1 = self.server.get_or_create_db(id1)
        db2 = self.server.get_or_create_db(id2)
        assert db1 is not db2

    def test_close_db(self):
        id_ = "path/to"
        db1 = self.server.get_or_create_db(id_)
        assert db1 is not None
        self.server.close_db(id_)
        assert not self.server.exists_db(id_)
