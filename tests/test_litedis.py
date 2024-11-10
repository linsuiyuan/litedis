import pytest
import time
from pathlib import Path
from litedis.litedis import Litedis
from litedis import PersistenceType


@pytest.fixture
def db():
    """创建测试用的数据库实例"""
    test_dir = Path("./test_data")
    db = Litedis(
        db_name="test_db",
        data_dir=str(test_dir),
        persistence=PersistenceType.MIXED
    )
    yield db
    # 清理测试数据
    del db
    if test_dir.exists():
        for file in test_dir.glob("*"):
            file.unlink()
        test_dir.rmdir()


class TestLitedis:
    """Litedis 基本功能测试"""

    def test_string_operations(self, db):
        """测试字符串操作"""
        # 测试 SET 和 GET
        assert db.set("name", "张三") is True
        assert db.get("name") == "张三"

        # 测试不存在的键
        assert db.get("nonexistent") is None

        # 测试带过期时间的 SET
        db.set("temp", "临时数据", ex=time.time() + 1)
        assert db.get("temp") == "临时数据"
        time.sleep(1.1)  # 等待过期
        assert db.get("temp") is None

    def test_list_operations(self, db):
        """测试列表操作"""
        # 测试 LPUSH 和 RPUSH
        assert db.lpush("list1", "a", "b") == 2
        assert db.rpush("list1", "c", "d") == 4

        # 测试 LRANGE
        assert db.lrange("list1", 0, -1) == ["b", "a", "c", "d"]

        # 测试 LPOP
        assert db.lpop("list1") == "b"
        assert db.lrange("list1", 0, -1) == ["a", "c", "d"]

        # 测试空列表
        assert db.lpop("empty_list") is None
        assert db.lrange("empty_list", 0, -1) == []

    def test_hash_operations(self, db):
        """测试哈希表操作"""
        # 测试 HSET
        assert db.hset("user:1", "name", "李四") == 1
        assert db.hset("user:1", "age", "25") == 1

        # 测试 HGET
        assert db.hget("user:1", "name") == "李四"
        assert db.hget("user:1", "age") == "25"
        assert db.hget("user:1", "nonexistent") is None

        # 测试 HGETALL
        assert db.hgetall("user:1") == {"name": "李四", "age": "25"}
        assert db.hgetall("nonexistent") == {}

    def test_set_operations(self, db):
        """测试集合操作"""
        # 测试 SADD
        assert db.sadd("set1", "a", "b", "c") == 3
        assert db.sadd("set1", "a", "d") == 1  # 只有 d 是新成员

        # 测试 SMEMBERS
        assert db.smembers("set1") == {"a", "b", "c", "d"}

        # 测试 SISMEMBER
        assert db.sismember("set1", "a") is True
        assert db.sismember("set1", "x") is False
        assert db.sismember("nonexistent", "a") is False

    def test_key_operations(self, db):
        """测试键操作"""
        # 测试 EXISTS
        db.set("key1", "value1")
        assert db.exists("key1") is True
        assert db.exists("nonexistent") is False

        # 测试 DELETE
        assert db.delete("key1") == 1
        assert db.exists("key1") is False

        # 测试多个键的删除
        db.set("k1", "v1")
        db.set("k2", "v2")
        assert db.delete("k1", "k2", "k3") == 2  # k3 不存在

    def test_expire(self, db):
        """测试过期功能"""
        db.set("temp_key", "temp_value")
        assert db.expire("temp_key", 1) is True
        assert db.exists("temp_key") is True
        time.sleep(1.1)
        assert db.exists("temp_key") is False

        # 测试对不存在的键设置过期时间
        assert db.expire("nonexistent", 1) is False

    def test_type_safety(self, db):
        """测试类型安全性"""
        # 字符串操作用于其他类型
        db.lpush("list1", "a")
        with pytest.raises(TypeError):
            db.hset("list1", "field", "value")

        # 哈希表操作用于其他类型
        db.set("str1", "value")
        with pytest.raises(TypeError):
            db.hset("str1", "field", "value")

        # 集合操作用于其他类型
        with pytest.raises(TypeError):
            db.sadd("str1", "member")
