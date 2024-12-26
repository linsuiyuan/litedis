import pytest

from refactor.db import LitedisDb


class TestLitedisDb:
    def setup_method(self):
        self.db = LitedisDb("data/db")
        self.string_obj = "bar"

    def test_set(self):
        assert not self.db.exists('foo')
        self.db.set('foo', self.string_obj)
        assert self.db.exists('foo')

    def test_check_value_type_consistency_on_set(self):
        self.db.set('foo', self.string_obj)
        with pytest.raises(TypeError):
            obj = ["bar"]
            self.db.set('foo', obj)

    def test_get(self):
        assert self.db.get('foo') is None
        self.db.set('foo', self.string_obj)
        assert self.db.get('foo') == self.string_obj

    def test_delete(self):
        assert self.db.exists('foo') == 0
        self.db.set('foo', self.string_obj)
        assert self.db.delete('foo') == 1

    def test_keys(self):
        self.db.set('foo', self.string_obj)
        assert list(self.db.keys()) == ['foo']

    def test_values(self):
        self.db.set('foo', self.string_obj)
        assert list(self.db.values()) == [self.string_obj]

    def test_set_and_get_expiration(self):
        self.db.set('foo', self.string_obj)
        self.db.set_expiration('foo', 666)
        assert self.db.get_expiration('foo') == 666

    def test_delete_expiration(self):
        self.db.set('foo', self.string_obj)
        self.db.set_expiration('foo', 666)
        assert self.db.get_expiration('foo') == 666
        self.db.delete_expiration('foo')
        assert self.db.get_expiration('foo') is None

    def test_get_expirations(self):
        self.db.set('foo', self.string_obj)
        self.db.set_expiration('foo', 666)
        assert self.db.get_expirations() == {'foo': 666}
