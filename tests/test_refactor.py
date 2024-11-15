import threading

import pytest  # noqa

from litedis import PersistenceType
from litedis.refactor import BasicKey
from litedis.refactor import ListType


class DB(ListType, BasicKey):
    """组合 mixin"""


@pytest.fixture
def temp_dir(tmp_path):
    """创建临时目录用于测试"""
    return tmp_path


@pytest.fixture
def db(temp_dir):
    """测试用的数据库数据"""
    db = DB()
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


class TestListType:
    def test_lpush_and_lrange(self, db):
        # 测试基本的 lpush 和 lrange 功能
        assert db.lpush("mylist", "world", "hello") == 2
        assert db.lrange("mylist", 0, -1) == ["hello", "world"]
        
        # 测试空列表的 lrange
        assert db.lrange("nonexistent", 0, -1) == []
    
    def test_rpush_and_lrange(self, db):
        # 测试基本的 rpush 功能
        assert db.rpush("mylist", "hello", "world") == 2
        assert db.lrange("mylist", 0, -1) == ["hello", "world"]
        
        # 测试向已存在的列表追加
        assert db.rpush("mylist", "!") == 3
        assert db.lrange("mylist", 0, -1) == ["hello", "world", "!"]
    
    def test_lpop(self, db):
        db.rpush("mylist", "one", "two", "three")
        
        # 测试单个弹出
        assert db.lpop("mylist") == "one"
        assert db.lrange("mylist", 0, -1) == ["two", "three"]
        
        # 测试多个弹出
        db.rpush("mylist", "four", "five")
        assert db.lpop("mylist", 2) == ["two", "three"]
        
        # 测试空列表弹出
        assert db.lpop("nonexistent") is None
    
    def test_rpop(self, db):
        db.rpush("mylist", "one", "two", "three")
        
        # 测试单个弹出
        assert db.rpop("mylist") == "three"
        assert db.lrange("mylist", 0, -1) == ["one", "two"]
        
        # 测试多个弹出
        db.rpush("mylist", "three", "four")
        assert db.rpop("mylist", 2) == ["four", "three"]
        
        # 测试空列表弹出
        assert db.rpop("nonexistent") is None
    
    def test_llen(self, db):
        # 测试空列表长度
        assert db.llen("nonexistent") == 0
        
        # 测试有内容的列表长度
        db.rpush("mylist", "one", "two", "three")
        assert db.llen("mylist") == 3
    
    def test_lindex(self, db):
        db.rpush("mylist", "one", "two", "three")
        
        # 测试正向索引
        assert db.lindex("mylist", 0) == "one"
        assert db.lindex("mylist", 2) == "three"
        
        # 测试负向索引
        assert db.lindex("mylist", -1) == "three"
        
        # 测试超出范围的索引
        assert db.lindex("mylist", 99) is None
        assert db.lindex("nonexistent", 0) is None
    
    def test_linsert(self, db):
        db.rpush("mylist", "one", "three")
        
        # 测试在指定值前插入
        assert db.linsert("mylist", "before", "three", "two") == 3
        assert db.lrange("mylist", 0, -1) == ["one", "two", "three"]
        
        # 测试在指定值后插入
        assert db.linsert("mylist", "after", "three", "four") == 4
        assert db.lrange("mylist", 0, -1) == ["one", "two", "three", "four"]
        
        # 测试插入不存在的参考值
        assert db.linsert("mylist", "before", "nonexistent", "value") == -1
    
    def test_lset(self, db):
        db.rpush("mylist", "one", "two", "three")
        
        # 测试设置已存在的索引
        assert db.lset("mylist", 1, "TWO") is True
        assert db.lrange("mylist", 0, -1) == ["one", "TWO", "three"]
        
        # 测试设置负数索引
        assert db.lset("mylist", -1, "THREE") is True
        assert db.lrange("mylist", 0, -1) == ["one", "TWO", "THREE"]
        
        # 测试索引超出范围
        with pytest.raises(IndexError):
            db.lset("mylist", 99, "value")
        
        # 测试不存在的键
        with pytest.raises(ValueError):
            db.lset("nonexistent", 0, "value")
    
    def test_ltrim(self, db):
        db.rpush("mylist", "one", "two", "three", "four", "five")
        
        # 测试基本修剪
        assert db.ltrim("mylist", 1, 3) is True
        assert db.lrange("mylist", 0, -1) == ["two", "three", "four"]
        
        # 测试负数索引修剪
        db.rpush("mylist2", "one", "two", "three", "four", "five")
        assert db.ltrim("mylist2", 1, -2) is True
        assert db.lrange("mylist2", 0, -1) == ["two", "three", "four"]
        
        # 测试不存在的键
        assert db.ltrim("nonexistent", 0, 1) is True
        assert db.lrange("nonexistent", 0, -1) == []
    
    def test_lsort(self, db):
        db.rpush("mylist", "3", "1", "2")
        
        # 测试基本排序
        assert db.lsort("mylist") == ["1", "2", "3"]
        
        # 测试降序排序
        assert db.lsort("mylist", desc=True) == ["3", "2", "1"]
        
        # 测试自定义排序
        db.rpush("mylist2", "abc", "a", "ab")
        assert db.lsort("mylist2", key=len) == ["a", "ab", "abc"]
        
        # 测试不存在的键
        assert db.lsort("nonexistent") == []
