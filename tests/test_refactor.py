import threading

import pytest  # noqa

from litedis import PersistenceType
from litedis.refactor import BasicKey


@pytest.fixture
def temp_dir(tmp_path):
    """创建临时目录用于测试"""
    return tmp_path


@pytest.fixture
def db(temp_dir):
    """测试用的数据库数据"""
    db = BasicKey()
    db.db_lock = threading.Lock()
    db.db_name = "test_db"
    db.data_dir = temp_dir
    db.persistence = PersistenceType.MIXED
    db.data = {}
    db.data_types = {}
    db.expires = {}
    return db


class TestBasicKey:
    def test_append(self, db):
        db.set("key", "Hello")
        length = db.append("key", " World")
        assert length == 11
        assert db.get("key") == "Hello World"

    def test_copy(self, db):
        db.set("source", "value")
        # 测试普通复制
        assert db.copy("source", "dest") is True
        assert db.get("dest") == "value"
        
        # 测试源键不存在
        assert db.copy("nonexistent", "dest2") is False
        
        # 测试目标键已存在且replace=False
        db.set("dest2", "original")
        assert db.copy("source", "dest2", replace=False) is False
        
        # 测试目标键已存在且replace=True
        assert db.copy("source", "dest2", replace=True) is True
        assert db.get("dest2") == "value"

    def test_decrby(self, db):
        db.set("counter", "10")
        assert db.decrby("counter") == 9
        assert db.decrby("counter", 5) == 4
        
        # 测试不存在的键
        assert db.decrby("new_counter") == -1

    def test_delete(self, db):
        db.set("key1", "value1")
        db.set("key2", "value2")
        
        assert db.delete("key1", "key2") == 2
        assert db.exists("key1") == 0
        assert db.exists("key2") == 0
        
        # 测试删除不存在的键
        assert db.delete("nonexistent") == 0

    def test_exists(self, db):
        db.set("key1", "value1")
        db.set("key2", "value2")
        
        assert db.exists("key1", "key2") == 2
        assert db.exists("key1", "nonexistent") == 1
        assert db.exists("nonexistent") == 0

    # todo 过期功能待以后测试
    # def test_expire(self, db):
    #     db.set("key", "value")
    #
    #     # 测试普通过期设置
    #     assert db.expire("key", 1) is True
    #     time.sleep(1.1)
    #     assert db.get("key") is None
    #
    #     # 测试 nx 选项
    #     db.set("key2", "value")
    #     db.expire("key2", 10)
    #     assert db.expire("key2", 5, nx=True) is False
    #
    #     # 测试 xx 选项
    #     db.set("key3", "value")
    #     assert db.expire("key3", 5, xx=True) is False

    def test_get_set(self, db):
        # 测试基本的设置和获取
        assert db.set("key", "value") is True
        assert db.get("key") == "value"
        
        # 测试 nx 选项
        assert db.set("key", "new_value", nx=True) is False
        assert db.get("key") == "value"
        
        # 测试 xx 选项
        assert db.set("nonexistent", "value", xx=True) is False
        
        # 测试 get 选项
        assert db.set("key", "newer_value", get=True) == "value"

    def test_incrby(self, db):
        # 测试基本增加
        assert db.incrby("counter") == 1
        assert db.incrby("counter", 5) == 6
        
        # 测试负数增加
        assert db.incrby("counter", -2) == 4

    def test_keys(self, db):
        db.set("test1", "value1")
        db.set("test2", "value2")
        db.set("other", "value3")
        
        # 测试通配符匹配
        assert set(db.keys("test*")) == {"test1", "test2"}
        assert len(db.keys("*")) == 3
        assert db.keys("nonexistent*") == []

    def test_mget_mset(self, db):
        # 测试 mset
        mapping = {"key1": "value1", "key2": "value2"}
        assert db.mset(mapping) is True
        
        # 测试 mget
        values = db.mget(["key1", "key2", "nonexistent"])
        assert values == ["value1", "value2"]

    def test_rename(self, db):
        db.set("old_key", "value")
        
        # 测试基本重命名
        assert db.rename("old_key", "new_key") is True
        assert db.get("new_key") == "value"
        assert db.exists("old_key") == 0
        
        # 测试重命名不存在的键
        with pytest.raises(AttributeError):
            db.rename("nonexistent", "new_key")

    def test_ttl(self, db):
        db.set("key", "value")
        db.expire("key", 5)
        
        # 测试 TTL
        assert db.ttl("key") <= 5
        assert db.ttl("key") > 0
        
        # 测试不存在的键
        assert db.ttl("nonexistent") == -2
        
        # 测试没有过期时间的键
        db.set("permanent", "value")
        assert db.ttl("permanent") == -1

    def test_type(self, db):
        db.set("string_key", "value")
        assert db.type("string_key") == "string"
        assert db.type("nonexistent") == "none"
