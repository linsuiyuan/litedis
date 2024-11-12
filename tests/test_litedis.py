import pytest
import time
from pathlib import Path
from litedis.litedis import Litedis
from litedis import PersistenceType


class TestLitedis:
    """Litedis 基本功能测试"""

    @pytest.fixture(autouse=True)
    def setup_db(self):
        """创建测试用的数据库实例"""
        test_dir = Path("./test_data")
        self.db = Litedis(
            db_name="test_db",
            data_dir=str(test_dir),
            persistence=PersistenceType.MIXED
        )
        yield
        # 清理测试数据
        self.db.close()
        if test_dir.exists():
            for file in test_dir.glob("*"):
                file.unlink()
            test_dir.rmdir()

    def test_connection_string(self):
        """创建测试用的数据库实例"""
        test_dir = Path("./connection")
        connection_string = "litedis:///connection/dbname"
        db = Litedis(connection_string=connection_string)

        assert test_dir.exists()
        db.set("connection_key1", "connection_value1")
        assert db.get("connection_key1") == "connection_value1"

        # 清理测试数据
        db.close()
        if test_dir.exists():
            for file in test_dir.glob("*"):
                file.unlink()
            test_dir.rmdir()

    def test_close(self):
        """测试关闭数据库及释放资源"""
        # 设置一些数据
        self.db.set("key1", "value1")
        self.db.set("key2", "value2")

        # 确保数据存在
        assert self.db.exists("key1") is True
        assert self.db.exists("key2") is True

        # 关闭数据库
        self.db.close()

        # 重新打开数据库，确保数据持久化
        self.db = Litedis(
            db_name="test_db",
            data_dir="./test_data",
            persistence=PersistenceType.MIXED
        )
        assert self.db.get("key1") == "value1"
        assert self.db.get("key2") == "value2"

    def test_delete(self):
        """测试 delete 命令"""
        # 测试删除存在的键
        self.db.set("key1", "value1")
        assert self.db.delete("key1") == 1
        assert self.db.exists("key1") is False
        assert self.db.get("key1") is None

        # 测试删除不存在的键
        assert self.db.delete("nonexistent") is 0

        # 测试删除带过期时间的键
        self.db.set("temp_key", "temp_value")
        self.db.expire("temp_key", 30)
        assert self.db.delete("temp_key") is 1
        assert self.db.exists("temp_key") is False

    def test_exists(self):
        """测试 exists 命令"""
        # 测试不存在的键
        assert self.db.exists("nonexistent") is False

        # 测试字符串类型键
        self.db.set("string_key", "value")
        assert self.db.exists("string_key") is True

        # 测试带过期时间的键
        self.db.set("temp_key", "temp_value")
        self.db.expire("temp_key", 1)
        assert self.db.exists("temp_key") is True
        time.sleep(1.1)
        assert self.db.exists("temp_key") is False

        # 测试删除后的键
        self.db.set("del_key", "value")
        assert self.db.exists("del_key") is True
        self.db.delete("del_key")
        assert self.db.exists("del_key") is False

    def test_expire(self):
        """测试过期功能"""
        self.db.set("temp_key", "temp_value")
        assert self.db.expire("temp_key", 1) is True
        assert self.db.exists("temp_key") is True
        time.sleep(1.1)
        assert self.db.exists("temp_key") is False

        # 测试对不存在的键设置过期时间
        assert self.db.expire("nonexistent", 1) is False

    # 字符串相关测试
    def test_set_and_get(self):
        """测试 set 和 get 命令"""
        # 测试基本的 set 和 get
        assert self.db.set("key1", "value1") is True
        assert self.db.get("key1") == "value1"

        # 测试覆盖已存在的值
        assert self.db.set("key1", "new_value") is True
        assert self.db.get("key1") == "new_value"

        # 测试获取不存在的键
        assert self.db.get("nonexistent") is None

        # 测试 get 类型错误
        self.db.zadd("zset_key", {"member": 1.0})
        with pytest.raises(TypeError, match="不是字符串"):
            self.db.get("zset_key")

    # 列表相关测试
    def test_lpush_and_rpush(self):
        """测试 lpush 和 rpush 命令"""
        # 测试 lpush
        assert self.db.lpush("list1", "value1") == 1
        assert self.db.lpush("list1", "value2", "value3") == 3
        assert self.db.lrange("list1", 0, -1) == ["value3", "value2", "value1"]

        # 测试 rpush
        assert self.db.rpush("list2", "value1") == 1
        assert self.db.rpush("list2", "value2", "value3") == 3
        assert self.db.lrange("list2", 0, -1) == ["value1", "value2", "value3"]

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="不是列表"):
            self.db.lpush("wrong_type", "value")
        with pytest.raises(TypeError, match="不是列表"):
            self.db.rpush("wrong_type", "value")

    def test_lpop_and_rpop(self):
        """测试 lpop 和 rpop 命令"""
        # 准备测试数据
        self.db.rpush("list1", "value1", "value2", "value3")

        # 测试 lpop
        assert self.db.lpop("list1") == "value1"
        assert self.db.lrange("list1", 0, -1) == ["value2", "value3"]

        # 测试 rpop
        assert self.db.rpop("list1") == "value3"
        assert self.db.lrange("list1", 0, -1) == ["value2"]

        # 测试空列表
        assert self.db.lpop("empty_list") is None
        assert self.db.rpop("empty_list") is None

        # 测试弹出最后一个元素
        assert self.db.lpop("list1") == "value2"
        assert self.db.lrange("list1", 0, -1) == []

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="不是列表"):
            self.db.lpop("wrong_type")
        with pytest.raises(TypeError, match="不是列表"):
            self.db.rpop("wrong_type")

    def test_lrange(self):
        """测试 lrange 命令"""
        # 准备测试数据
        self.db.rpush("list1", "one", "two", "three", "four", "five")

        # 测试正常范围
        assert self.db.lrange("list1", 0, 2) == ["one", "two", "three"]
        assert self.db.lrange("list1", 1, 3) == ["two", "three", "four"]

        # 测试负数索引
        assert self.db.lrange("list1", 0, -1) == ["one", "two", "three", "four", "five"]
        assert self.db.lrange("list1", -3, -1) == ["three", "four", "five"]

        # 测试越界索引
        assert self.db.lrange("list1", 0, 10) == ["one", "two", "three", "four", "five"]
        assert self.db.lrange("list1", -10, 2) == ["one", "two", "three"]

        # 测试空列表
        assert self.db.lrange("nonexistent", 0, -1) == []

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="不是列表"):
            self.db.lrange("wrong_type", 0, -1)

    def test_llen(self):
        """测试 llen 命令"""
        # 测试空列表
        assert self.db.llen("nonexistent") == 0

        # 测试单个元素
        self.db.lpush("list1", "value1")
        assert self.db.llen("list1") == 1

        # 测试多个元素
        self.db.rpush("list2", "one", "two", "three")
        assert self.db.llen("list2") == 3

        # 测试弹出后的长度变化
        self.db.lpop("list2")
        assert self.db.llen("list2") == 2
        self.db.rpop("list2")
        assert self.db.llen("list2") == 1

        # 测试清空后的长度
        self.db.lpop("list2")
        assert self.db.llen("list2") == 0

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="string 不是列表"):
            self.db.llen("wrong_type")

    # 哈希相关测试
    def test_hset_and_hget(self):
        """测试 hset 和 hget 命令"""
        # 测试基本的 hset 和 hget
        assert self.db.hset("hash1", "field1", "value1") == 1
        assert self.db.hget("hash1", "field1") == "value1"

        # 测试更新已存在的字段
        assert self.db.hset("hash1", "field1", "new_value") == 0
        assert self.db.hget("hash1", "field1") == "new_value"

        # 测试获取不存在的字段
        assert self.db.hget("hash1", "nonexistent") is None

        # 测试获取不存在的键
        assert self.db.hget("nonexistent", "field1") is None

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="不是哈希"):
            self.db.hset("wrong_type", "field1", "value1")

    def test_hgetall(self):
        """测试 hgetall 命令"""
        # 测试空哈希表
        assert self.db.hgetall("nonexistent") == {}

        # 测试单个字段
        self.db.hset("hash1", "field1", "value1")
        assert self.db.hgetall("hash1") == {"field1": "value1"}

        # 测试多个字段
        self.db.hset("hash1", "field2", "value2")
        self.db.hset("hash1", "field3", "value3")
        expected = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        assert self.db.hgetall("hash1") == expected

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="不是哈希"):
            self.db.hgetall("wrong_type")

    # 集合相关测试
    def test_sadd(self):
        """测试 sadd 命令"""
        # 测试添加新成员
        assert self.db.sadd("set1", "member1") == 1
        assert self.db.sadd("set1", "member2", "member3") == 2

        # 测试添加重复成员
        assert self.db.sadd("set1", "member1", "member2") == 0

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="不是集合"):
            self.db.sadd("wrong_type", "member1")

    def test_smembers(self):
        """测试 smembers 命令"""
        # 测试空集合
        assert self.db.smembers("nonexistent") == set()

        # 测试有成员的集合
        self.db.sadd("set1", "member1", "member2", "member3")
        expected = {"member1", "member2", "member3"}
        assert self.db.smembers("set1") == expected

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="不是集合"):
            self.db.smembers("wrong_type")

    def test_sismember(self):
        """测试 sismember 命令"""
        # 准备测试数据
        self.db.sadd("set1", "member1", "member2", "member3")

        # 测试存在的成员
        assert self.db.sismember("set1", "member1") is True
        assert self.db.sismember("set1", "member2") is True

        # 测试不存在的成员
        assert self.db.sismember("set1", "nonexistent") is False

        # 测试不存在的集合
        assert self.db.sismember("nonexistent", "member1") is False

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="不是集合"):
            self.db.sismember("wrong_type", "member1")

    # 有序集合相关测试
    def test_zadd(self):
        """测试zadd命令"""
        # 测试添加新成员
        assert self.db.zadd("scores", {"Alice": 89.5, "Bob": 92.0}) == 2

        # 测试更新已存在成员的分数
        assert self.db.zadd("scores", {"Bob": 95.0}) == 1
        assert self.db.zscore("scores", "Bob") == 95.0

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="string 不是有序集合"):
            self.db.zadd("wrong_type", {"Alice": 89.5})

    def test_zscore(self):
        """测试zscore命令"""
        # 添加测试数据
        self.db.zadd("scores", {"Alice": 89.5, "Bob": 92.0})

        # 测试获取存在的成员分数
        assert self.db.zscore("scores", "Alice") == 89.5
        assert self.db.zscore("scores", "Bob") == 92.0

        # 测试获取不存在的成员
        assert self.db.zscore("scores", "Charlie") is None

        # 测试获取不存在的键
        assert self.db.zscore("not_exists", "Alice") is None

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="string 不是有序集合"):
            self.db.zscore("wrong_type", "Alice")

    def test_zrange(self):
        """测试zrange命令"""
        # 添加测试数据
        self.db.zadd("scores", {
            "Alice": 89.5,
            "Bob": 92.0,
            "Charlie": 78.5,
            "David": 95.0
        })

        # 测试正常范围查询
        assert self.db.zrange("scores", 0, 2) == ["Charlie", "Alice", "Bob"]

        # 测试带分数的查询
        result = self.db.zrange("scores", 0, 2, withscores=True)
        expected = [
            ("Charlie", 78.5),
            ("Alice", 89.5),
            ("Bob", 92.0)
        ]
        assert result == expected

        # 测试负数索引
        assert self.db.zrange("scores", 0, -1) == [
            "Charlie", "Alice", "Bob", "David"
        ]

        # 测试空结果
        assert self.db.zrange("not_exists", 0, -1) == []

        # 测试类型错误
        self.db.set("wrong_type", "string")
        with pytest.raises(TypeError, match="string 不是有序集合"):
            self.db.zrange("wrong_type", 0, -1)
