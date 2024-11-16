import threading

import pytest  # noqa

from litedis import PersistenceType
from litedis.refactor import (
    BasicKey,
    ListType,
    SetType,
    SortedSetType,
)


class DB(
    SortedSetType,
    SetType,
    ListType,
    BasicKey,
):
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


class TestSetType:
    def test_sadd_and_smembers(self, db):
        # 测试基本的添加功能
        assert db.sadd("myset", "one") == 1
        assert db.sadd("myset", "two", "three") == 2
        
        # 测试添加重复元素
        assert db.sadd("myset", "one") == 0
        
        # 测试获取所有成员
        assert db.smembers("myset") == {"one", "two", "three"}
        
        # 测试不存在的集合
        assert db.smembers("nonexistent") == set()

    def test_scard(self, db):
        # 测试空集合
        assert db.scard("nonexistent") == 0
        
        # 测试有成员的集合
        db.sadd("myset", "one", "two", "three")
        assert db.scard("myset") == 3
        
        # 测试添加后的计数
        db.sadd("myset", "four")
        assert db.scard("myset") == 4

    def test_sdiff(self, db):
        # 准备测试数据
        db.sadd("set1", "a", "b", "c")
        db.sadd("set2", "b", "c", "d")
        db.sadd("set3", "c", "d", "e")
        
        # 测试两个集合的差集
        assert db.sdiff(["set1", "set2"]) == {"a"}
        
        # 测试多个集合的差集
        assert db.sdiff(["set1", "set2", "set3"]) == {"a"}
        
        # 测试包含不存在的集合
        assert db.sdiff(["set1", "nonexistent"]) == {"a", "b", "c"}

    def test_sinter(self, db):
        # 准备测试数据
        db.sadd("set1", "a", "b", "c")
        db.sadd("set2", "b", "c", "d")
        db.sadd("set3", "c", "d", "e")
        
        # 测试两个集合的交集
        assert db.sinter(["set1", "set2"]) == {"b", "c"}
        
        # 测试多个集合的交集
        assert db.sinter(["set1", "set2", "set3"]) == {"c"}
        
        # 测试包含不存在的集合
        assert db.sinter(["set1", "nonexistent"]) == set()

    def test_sismember_and_smismember(self, db):
        db.sadd("myset", "one", "two", "three")
        
        # 测试单个成员检查
        assert db.sismember("myset", "one") is True
        assert db.sismember("myset", "four") is False
        assert db.sismember("nonexistent", "one") is False
        
        # 测试多个成员检查
        assert db.smismember("myset", ["one", "four", "two"]) == [True, False, True]
        assert db.smismember("nonexistent", ["one", "two"]) == []

    def test_smove(self, db):
        db.sadd("source", "one", "two")
        db.sadd("destination", "three")
        
        # 测试成功移动
        assert db.smove("source", "destination", "two") is True
        assert db.smembers("source") == {"one"}
        assert db.smembers("destination") == {"two", "three"}
        
        # 测试移动不存在的成员
        assert db.smove("source", "destination", "nonexistent") is False
        
        # 测试从不存在的集合移动
        assert db.smove("nonexistent", "destination", "one") is False

    def test_spop(self, db):
        db.sadd("myset", "one", "two", "three", "four")
        
        # 测试弹出单个元素
        popped = db.spop("myset")
        assert popped in {"one", "two", "three", "four"}
        assert db.scard("myset") == 3
        
        # 测试弹出多个元素
        popped_multiple = db.spop("myset", 2)
        assert len(popped_multiple) == 2
        assert db.scard("myset") == 1
        
        # 测试空集合弹出
        assert db.spop("nonexistent") is None

    def test_srandmember(self, db):
        db.sadd("myset", "one", "two", "three", "four")
        
        # 测试获取单个随机成员
        member = db.srandmember("myset")
        assert member in {"one", "two", "three", "four"}
        
        # 测试获取多个不重复随机成员
        members = db.srandmember("myset", 2)
        assert len(members) == 2
        assert len(set(members)) == 2  # 确保没有重复
        
        # 测试获取可能重复的随机成员
        members = db.srandmember("myset", -2)
        assert len(members) == 2
        
        # 测试空集合
        assert db.srandmember("nonexistent") is None
        assert db.srandmember("nonexistent", 2) is None

    def test_srem(self, db):
        db.sadd("myset", "one", "two", "three")
        
        # 测试移除单个成员
        assert db.srem("myset", "one") == 1
        assert db.smembers("myset") == {"two", "three"}
        
        # 测试移除多个成员
        assert db.srem("myset", "two", "three", "nonexistent") == 2
        assert db.smembers("myset") == set()
        
        # 测试移除不存在集合的成员
        assert db.srem("nonexistent", "one") == 0

    def test_sunion(self, db):
        # 准备测试数据
        db.sadd("set1", "a", "b", "c")
        db.sadd("set2", "c", "d", "e")
        db.sadd("set3", "e", "f", "g")
        
        # 测试两个集合的并集
        assert db.sunion(["set1", "set2"]) == {"a", "b", "c", "d", "e"}
        
        # 测试多个集合的并集
        assert db.sunion(["set1", "set2", "set3"]) == {"a", "b", "c", "d", "e", "f", "g"}
        
        # 测试包含不存在的集合
        assert db.sunion(["set1", "nonexistent"]) == {"a", "b", "c"}


class TestSortedSetType:
    def test_zadd_and_zscore(self, db):
        # 测试基本添加功能
        assert db.zadd("myset", {"one": 1, "two": 2, "three": 3}) == 3
        assert db.zscore("myset", "one") == 1.0
        assert db.zscore("myset", "two") == 2.0
        
        # 测试更新分数
        assert db.zadd("myset", {"one": 1.5}) == 0
        assert db.zscore("myset", "one") == 1.5
        
        # 测试 nx 选项 (只添加新元素)
        assert db.zadd("myset", {"one": 2.0}, nx=True) == 0
        assert db.zscore("myset", "one") == 1.5
        
        # 测试 xx 选项 (只更新已存在的元素)
        assert db.zadd("myset", {"four": 4}, xx=True) == 0
        assert db.zscore("myset", "four") is None
        
        # 测试 gt 选项 (只在新分数大于当前分数时更新)
        assert db.zadd("myset", {"one": 1.0}, gt=True) == 0
        assert db.zscore("myset", "one") == 1.5
        assert db.zadd("myset", {"one": 2.0}, gt=True) == 0
        assert db.zscore("myset", "one") == 2.0
        
        # 测试 lt 选项 (只在新分数小于当前分数时更新)
        assert db.zadd("myset", {"one": 2.5}, lt=True) == 0
        assert db.zscore("myset", "one") == 2.0
        assert db.zadd("myset", {"one": 1.5}, lt=True) == 0
        assert db.zscore("myset", "one") == 1.5

    def test_zcard_and_zcount(self, db):
        db.zadd("myset", {"one": 1, "two": 2, "three": 3, "four": 4})
        
        # 测试集合基数
        assert db.zcard("myset") == 4
        assert db.zcard("nonexistent") == 0
        
        # 测试计数范围
        assert db.zcount("myset", 2, 3) == 2
        assert db.zcount("myset", 1, 4) == 4
        assert db.zcount("myset", 5, 6) == 0
        assert db.zcount("nonexistent", 1, 2) == 0

    def test_zdiff_and_zinter(self, db):
        # 准备测试数据
        db.zadd("set1", {"a": 1, "b": 2, "c": 3})
        db.zadd("set2", {"b": 2, "c": 3, "d": 4})
        
        # 测试差集
        assert db.zdiff(["set1", "set2"]) == ["a"]
        assert db.zdiff(["set1", "set2"], withscores=True) == [("a", 1.0)]
        
        # 测试交集
        assert set(db.zinter(["set1", "set2"])) == {"b", "c"}
        result = db.zinter(["set1", "set2"], withscores=True)
        assert len(result) == 2  # 2个元素 * (元素+分数)

    def test_zincrby(self, db):
        # 测试基本增量
        assert db.zincrby("myset", 2.0, "member") == 2.0
        assert db.zincrby("myset", 3.0, "member") == 5.0
        
        # 测试负增量
        assert db.zincrby("myset", -1.0, "member") == 4.0
        
        # 测试新成员
        assert db.zincrby("myset", 1.0, "newmember") == 1.0

    def test_zpopmax_and_zpopmin(self, db):
        db.zadd("myset", {"one": 1, "two": 2, "three": 3, "four": 4})
        
        # 测试弹出最大值
        assert db.zpopmax("myset") == ["four", 4.0]
        assert db.zpopmax("myset", 2) == ["three", 3.0, "two", 2.0]
        
        # 测试弹出最小值
        db.zadd("myset2", {"one": 1, "two": 2, "three": 3, "four": 4})
        assert db.zpopmin("myset2") == ["one", 1.0]
        assert db.zpopmin("myset2", 2) == ["two", 2.0, "three", 3.0]

    def test_zrange_and_zrevrange(self, db):
        db.zadd("myset", {"one": 1, "two": 2, "three": 3, "four": 4})
        
        # 测试正序范围查询
        assert db.zrange("myset", 0, -1) == ["one", "two", "three", "four"]
        assert db.zrange("myset", 1, 2) == ["two", "three"]
        assert db.zrange("myset", 0, -1, withscores=True) == [
            "one", 1.0, "two", 2.0, "three", 3.0, "four", 4.0
        ]
        
        # 测试逆序范围查询
        assert db.zrevrange("myset", 0, -1) == ["four", "three", "two", "one"]
        assert db.zrevrange("myset", 1, 2) == ["three", "two"]
        assert db.zrevrange("myset", 0, -1, withscores=True) == [
            "four", 4.0, "three", 3.0, "two", 2.0, "one", 1.0
        ]

    def test_zrangebyscore_and_zrevrangebyscore(self, db):
        db.zadd("myset", {"one": 1, "two": 2, "three": 3, "four": 4})
        
        # 测试按分数范围查询
        assert db.zrangebyscore("myset", 2, 3) == ["two", "three"]
        assert db.zrangebyscore("myset", 2, 3, withscores=True) == ["two", 2.0, "three", 3.0]
        
        # 测试按分数范围逆序查询
        assert db.zrevrangebyscore("myset", 2, 3) == ["three", "two"]
        assert db.zrevrangebyscore("myset", 2, 3, withscores=True) == ["three", 3.0, "two", 2.0]
        
        # 测试带起始位置和数量的查询
        assert db.zrangebyscore("myset", 1, 4, start=1, num=2) == ["two", "three", "four"]
        assert db.zrevrangebyscore("myset", 1, 4, start=1, num=2) == ["three", "two", "one"]

    def test_zrank_and_zrevrank(self, db):
        db.zadd("myset", {"one": 1, "two": 2, "three": 3, "four": 4})
        
        # 测试获取排名
        assert db.zrank("myset", "one") == 0
        assert db.zrank("myset", "four") == 3
        assert db.zrank("myset", "nonexistent") is None
        
        # 测试获取逆序排名
        assert db.zrevrank("myset", "four") == 0
        assert db.zrevrank("myset", "one") == 3
        assert db.zrevrank("myset", "nonexistent") is None

    def test_zrem_and_zremrangebyscore(self, db):
        db.zadd("myset", {"one": 1, "two": 2, "three": 3, "four": 4})
        
        # 测试删除成员
        assert db.zrem("myset", "one", "two") == 2
        assert db.zcard("myset") == 2
        assert db.zrem("myset", "nonexistent") == 0
        
        # 测试按分数范围删除
        db.zadd("myset2", {"one": 1, "two": 2, "three": 3, "four": 4})
        assert db.zremrangebyscore("myset2", 2, 3) == 2
        assert db.zcard("myset2") == 2
        assert set(db.zrange("myset2", 0, -1)) == {"one", "four"}

    def test_zunion(self, db):
        db.zadd("set1", {"a": 1, "b": 2, "c": 3})
        db.zadd("set2", {"b": 4, "c": 5, "d": 6})
        
        # 测试并集
        result = set(db.zunion(["set1", "set2"]))
        assert result == {"a", "b", "c", "d"}
        
        # 测试带分数的并集
        result = db.zunion(["set1", "set2"], withscores=True)
        assert len(result) == 4  # 4个元素 * (元素+分数)

    def test_zmscore(self, db):
        db.zadd("myset", {"one": 1, "two": 2, "three": 3})
        
        # 测试获取多个成员分数
        assert db.zmscore("myset", ["one", "three", "nonexistent"]) == [1.0, 3.0, None]
        assert db.zmscore("nonexistent", ["one", "two"]) == [None, None]

    def test_zrandmember(self, db):
        db.zadd("myset", {"one": 1, "two": 2, "three": 3, "four": 4})
        
        # 测试随机获取单个成员
        member = db.zrandmember("myset")
        assert member in ["one", "two", "three", "four"]
        
        # 测试随机获取多个不重复成员
        members = db.zrandmember("myset", 2)
        assert len(members) == 2
        assert len(set(members)) == 2
        
        # 测试随机获取可能重复的成员
        members = db.zrandmember("myset", -2)
        assert len(members) == 2
        
        # 测试带分数获取
        result = db.zrandmember("myset", 2, withscores=True)
        assert len(result) == 4  # 2个元素 * (元素+分数)

    def test_zmpop(self, db):
        db.zadd("set1", {"a": 1, "b": 2, "c": 3})
        db.zadd("set2", {"d": 4, "e": 5, "f": 6})
        
        # 测试弹出最小值
        result = db.zmpop(2, ["set1", "set2"], min_=True)
        assert result[0] == "set1"  # 从第一个非空集合弹出
        assert result[1] == ["a", 1.0]
        
        # 测试弹出最大值
        result = db.zmpop(2, ["set1", "set2"], max_=True)
        assert result[0] == "set1"
        assert result[1] == ["c", 3.0]
        
        # 测试空集合
        result = db.zmpop(1, ["nonexistent"], min_=True)
        assert result == []
