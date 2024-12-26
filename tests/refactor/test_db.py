import pytest

from refactor.db import LitedisDb, LitedisObject


class TestLitedisDb:
    def setup_method(self):
        self.litedis_db = LitedisDb()
        self.string_obj = LitedisObject("bar")

    def test_set(self):
        assert not self.litedis_db.exists('foo')
        self.litedis_db.set('foo', self.string_obj)
        assert self.litedis_db.exists('foo')

    def test_check_value_type_consistency_on_set(self):
        self.litedis_db.set('foo', self.string_obj)
        with pytest.raises(TypeError):
            obj = LitedisObject(["bar"])
            self.litedis_db.set('foo', obj)

    def test_get(self):
        assert self.litedis_db.get('foo') is None
        self.litedis_db.set('foo', self.string_obj)
        assert self.litedis_db.get('foo') == self.string_obj

    def test_delete(self):
        assert self.litedis_db.exists('foo') == 0
        self.litedis_db.set('foo', self.string_obj)
        assert self.litedis_db.delete('foo') == 1

    def test_keys(self):
        self.litedis_db.set('foo', self.string_obj)
        assert list(self.litedis_db.keys()) == ['foo']

    def test_values(self):
        self.litedis_db.set('foo', self.string_obj)
        assert list(self.litedis_db.values()) == [self.string_obj]

    def test_set_and_get_expiration(self):
        self.litedis_db.set('foo', self.string_obj)
        self.litedis_db.set_expiration('foo', 666)
        assert self.litedis_db.get_expiration('foo') == 666

    def test_get_expirations(self):
        self.litedis_db.set('foo', self.string_obj)
        self.litedis_db.set_expiration('foo', 666)
        assert self.litedis_db.get_expirations() == {'foo': 666}
