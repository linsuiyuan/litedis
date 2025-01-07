from collections import defaultdict
from threading import Lock
from unittest import SkipTest

import pytest

from refactor2.client.litedis import Litedis
from refactor2.core.dbmanager import DBManager


class BaseTest:
    """Base test class with common fixtures and setup"""

    @pytest.fixture(autouse=True)
    def reset_dbmanager(self):
        """Reset DBManager state before each test"""
        DBManager._dbs = {}
        DBManager._dbs_lock = Lock()
        DBManager._db_locks = defaultdict(Lock)
        DBManager._instances = {}
        yield

    @pytest.fixture
    def temp_path(self, tmp_path_factory):
        """Create a temporary directory for test data"""
        return tmp_path_factory.mktemp("litedis_test")

    @pytest.fixture
    def client(self, temp_path):
        """Create a fresh Litedis client for each test"""
        return Litedis(dbname="test", data_path=temp_path)


class TestBasicCommands(BaseTest):

    def test_set_get(self, client):
        assert client.execute("set", "key1", "value1") == "OK"
        assert client.execute("get", "key1") == "value1"
        assert client.execute("get", "nonexistent") is None

    def test_append(self, client):
        assert client.append("key1", "Hello") == 5
        assert client.append("key1", " World") == 11
        assert client.get("key1") == "Hello World"

    def test_copy(self, client):
        client.set("source", "value")
        assert client.copy("source", "dest") == 1
        assert client.get("dest") == "value"

        # Test with replace option
        client.set("dest", "old_value")
        assert client.copy("source", "dest", replace=True) == 1
        assert client.get("dest") == "value"

    def test_decrby(self, client):
        client.set("counter", "10")
        assert client.decrby("counter", 3) == 7
        assert client.decrby("counter", 5) == 2
        assert client.decrby("nonexistent", 5) == -5

    def test_delete(self, client):
        client.set("key1", "value1")
        client.set("key2", "value2")
        assert client.delete("key1", "key2", "nonexistent") == 2
        assert client.get("key1") is None
        assert client.get("key2") is None

    def test_exists(self, client):
        client.set("key1", "value1")
        client.set("key2", "value2")
        assert client.exists("key1", "key2", "nonexistent") == 2

    def test_expire_expireat(self, client):
        """Test EXPIRE and EXPIREAT commands"""
        client.set("key1", "value1")
        assert client.expire("key1", 100) == 1
        assert 0 < client.ttl("key1") <= 100

        client.set("key2", "value2")
        import time
        future = int(time.time()) + 100
        assert client.expireat("key2", future) == 1
        assert 0 < client.ttl("key2") <= 100

        # Test with NX, XX, GT, LT options
        assert client.expire("key1", 200, nx=True) == 0  # Already has expiry
        assert client.expire("key1", 200, gt=True) == 1  # New expiry is greater
        assert client.expire("key1", 50, lt=True) == 1   # New expiry is less

    def test_expiretime(self, client):
        client.set("key1", "value1")
        import time
        future = int(time.time()) + 100
        client.expireat("key1", future)
        assert client.expiretime("key1") == future

    def test_incrby_incrbyfloat(self, client):
        # Test INCRBY
        assert client.incrby("counter", 5) == 5
        assert client.incrby("counter", 3) == 8

        # Test INCRBYFLOAT
        assert client.incrbyfloat("float_counter", 1.5) == 1.5
        assert client.incrbyfloat("float_counter", 2.1) == 3.6

    def test_keys(self, client):
        client.set("key1", "value1")
        client.set("key2", "value2")
        client.set("other", "value3")

        keys = client.keys("key*")
        assert len(keys) == 2
        assert "key1" in keys
        assert "key2" in keys

    def test_mget_mset(self, client):
        # Test MSET
        assert client.mset({"key1": "value1", "key2": "value2"}) == "OK"

        # Test MGET
        assert client.mget("key1", "key2", "nonexistent") == ["value1", "value2", None]

    def test_msetnx(self, client):
        assert client.msetnx({"key1": "value1", "key2": "value2"}) == 1
        assert client.msetnx({"key2": "new_value", "key3": "value3"}) == 0
        assert client.get("key2") == "value2"  # Original value preserved
        assert client.get("key3") is None  # Not set

    def test_persist(self, client):
        client.set("key1", "value1")
        client.expire("key1", 100)
        assert client.persist("key1") == 1
        assert client.ttl("key1") == -1

    def test_randomkey(self, client):
        client.set("key1", "value1")
        client.set("key2", "value2")
        assert client.randomkey() in ["key1", "key2"]

        client.delete("key1", "key2")
        assert client.randomkey() is None

    def test_rename_renamenx(self, client):
        client.set("source", "value")

        # Test RENAME
        assert client.rename("source", "dest") == "OK"
        assert client.get("dest") == "value"
        assert client.exists("source") == 0

        # Test RENAMENX
        client.set("source", "value1")
        client.set("dest", "value2")
        assert client.renamenx("source", "dest") == 0
        assert client.get("dest") == "value2"

    def test_strlen(self, client):
        client.set("key1", "Hello World")
        assert client.strlen("key1") == 11
        assert client.strlen("nonexistent") == 0

    def test_substr(self, client):
        client.set("key1", "Hello World")
        assert client.substr("key1", 0, 4) == "Hello"
        assert client.substr("key1", -5, -1) == "World"

    def test_ttl(self, client):
        client.set("key1", "value1")
        client.expire("key1", 100)
        assert 0 < client.ttl("key1") <= 100
        assert client.ttl("nonexistent") == -2

    def test_type(self, client):
        client.set("string_key", "value")
        assert client.type("string_key") == "string"
        assert client.type("nonexistent") == "none"


class TestHashCommands(BaseTest):
    def test_hdel(self, client):
        client.hset("hash1", {"field1": "value1", "field2": "value2"})
        assert client.hdel("hash1", "field1", "field2", "nonexistent") == 2
        assert client.hget("hash1", "field1") is None
        assert client.hget("hash1", "field2") is None

    def test_hexists(self, client):
        client.hset("hash1", {"field1": "value1"})
        assert client.hexists("hash1", "field1") == 1
        assert client.hexists("hash1", "nonexistent") == 0
        assert client.hexists("nonexistent_hash", "field1") == 0

    def test_hget(self, client):
        client.hset("hash1", {"field1": "value1"})
        assert client.hget("hash1", "field1") == "value1"
        assert client.hget("hash1", "nonexistent") is None
        assert client.hget("nonexistent_hash", "field1") is None

    def test_hgetall(self, client):
        client.hset("hash1", {"field1": "value1", "field2": "value2"})
        result = client.hgetall("hash1")
        assert len(result) == 4  # [field1, value1, field2, value2]
        assert "field1" in result
        assert "value1" in result
        assert "field2" in result
        assert "value2" in result

    def test_hincrby(self, client):
        assert client.hincrby("hash1", "counter", 5) == 5
        assert client.hincrby("hash1", "counter", 3) == 8
        assert client.hincrby("hash1", "counter", -2) == 6
        assert client.hget("hash1", "counter") == '6'

    def test_hincrbyfloat(self, client):
        assert float(client.hincrbyfloat("hash1", "counter", 1.5)) == 1.5
        assert float(client.hincrbyfloat("hash1", "counter", 2.1)) == 3.6
        assert float(client.hincrbyfloat("hash1", "counter", -1.1)) == 2.5
        assert float(client.hget("hash1", "counter")) == 2.5

    def test_hkeys(self, client):
        client.hset("hash1", {"field1": "value1", "field2": "value2"})
        keys = client.hkeys("hash1")
        assert len(keys) == 2
        assert "field1" in keys
        assert "field2" in keys
        assert client.hkeys("nonexistent") == []

    def test_hlen(self, client):
        client.hset("hash1", {"field1": "value1", "field2": "value2"})
        assert client.hlen("hash1") == 2
        assert client.hlen("nonexistent") == 0

    def test_hmget(self, client):
        client.hset("hash1", {"field1": "value1", "field2": "value2"})
        result = client.hmget("hash1", "field1", "field2", "nonexistent")
        assert result == ["value1", "value2", None]
        assert client.hmget("nonexistent", "field1") == [None]

    def test_hset(self, client):
        # Single field-value pair
        assert client.hset("hash1", {"field1": "value1"}) == 1
        assert client.hget("hash1", "field1") == "value1"

        # Multiple field-value pairs
        assert client.hset("hash1", {
            "field2": "value2",
            "field3": "value3"
        }) == 2
        assert client.hget("hash1", "field2") == "value2"
        assert client.hget("hash1", "field3") == "value3"

        # Update existing field
        assert client.hset("hash1", {"field1": "new_value"}) == 0
        assert client.hget("hash1", "field1") == "new_value"

    def test_hsetnx(self, client):
        assert client.hsetnx("hash1", "field1", "value1") == 1
        assert client.hsetnx("hash1", "field1", "value2") == 0
        assert client.hget("hash1", "field1") == "value1"

    def test_hstrlen(self, client):
        client.hset("hash1", {"field1": "Hello World"})
        assert client.hstrlen("hash1", "field1") == 11
        assert client.hstrlen("hash1", "nonexistent") == 0
        assert client.hstrlen("nonexistent", "field1") == 0

    def test_hvals(self, client):
        client.hset("hash1", {"field1": "value1", "field2": "value2"})
        values = client.hvals("hash1")
        assert len(values) == 2
        assert "value1" in values
        assert "value2" in values
        assert client.hvals("nonexistent") == []

    def test_hscan(self, client):
        # Populate hash with test data
        test_data = {f"field{i}": f"value{i}" for i in range(10)}
        client.hset("hash1", test_data)

        # Test basic scan
        cursor, results = client.hscan("hash1", 0)
        assert len(results) > 0

        # Test with match pattern
        cursor, results = client.hscan("hash1", 0, match="field[0-4]*")
        matching_fields = [results[i] for i in range(0, len(results), 2)]
        assert all(f.startswith("field") and int(f[5]) < 5 for f in matching_fields)

        # Test with count
        cursor, results = client.hscan("hash1", 0, count=5)
        assert len(results) <= 10  # 5 field-value pairs = 10 items

        # Test scanning nonexistent hash
        cursor, results = client.hscan("nonexistent", 0)
        assert cursor == 0
        assert len(results) == 0


# todo to fix
@SkipTest
class TestListCommands(BaseTest):

    def test_lindex(self, client):
        """Test LINDEX command"""
        client.rpush("list1", "value1", "value2", "value3")
        assert client.lindex("list1", 0) == "value1"
        assert client.lindex("list1", -1) == "value3"
        assert client.lindex("list1", 3) is None
        assert client.lindex("nonexistent", 0) is None

    def test_linsert(self, client):
        """Test LINSERT command"""
        client.rpush("list1", "value1", "value2", "value3")
        # Insert before
        assert client.linsert("list1", True, "value2", "new1") == 4
        assert client.lrange("list1", 0, -1) == ["value1", "new1", "value2", "value3"]

        # Insert after
        assert client.linsert("list1", False, "value2", "new2") == 5
        assert client.lrange("list1", 0, -1) == ["value1", "new1", "value2", "new2", "value3"]

        # Insert with nonexistent pivot
        assert client.linsert("list1", True, "nonexistent", "new3") == -1

        # Insert into nonexistent list
        assert client.linsert("nonexistent", True, "value", "new") == 0

    def test_llen(self, client):
        """Test LLEN command"""
        assert client.llen("nonexistent") == 0
        client.rpush("list1", "value1", "value2", "value3")
        assert client.llen("list1") == 3

    def test_lpop(self, client):
        """Test LPOP command"""
        client.rpush("list1", "value1", "value2", "value3")
        # Single pop
        assert client.lpop("list1") == "value1"

        # Multiple pop
        client.rpush("list2", "value1", "value2", "value3")
        assert client.lpop("list2", 2) == ["value1", "value2"]

        # Pop from empty list
        assert client.lpop("nonexistent") is None

        # Pop more than available
        assert len(client.lpop("list2", 5)) == 1  # Only one element left

    def test_lpush(self, client):
        """Test LPUSH command"""
        assert client.lpush("list1", "value1") == 1
        assert client.lpush("list1", "value2", "value3") == 3
        assert client.lrange("list1", 0, -1) == ["value3", "value2", "value1"]

    def test_lpushx(self, client):
        """Test LPUSHX command"""
        # Push to non-existing list
        assert client.lpushx("list1", "value1") == 0
        assert client.llen("list1") == 0

        # Push to existing list
        client.lpush("list1", "value1")
        assert client.lpushx("list1", "value2", "value3") == 3
        assert client.lrange("list1", 0, -1) == ["value3", "value2", "value1"]

    def test_lrange(self, client):
        """Test LRANGE command"""
        client.rpush("list1", "value1", "value2", "value3", "value4", "value5")
        assert client.lrange("list1", 0, 2) == ["value1", "value2", "value3"]
        assert client.lrange("list1", -3, -1) == ["value3", "value4", "value5"]
        assert client.lrange("list1", 3, 1) == []
        assert client.lrange("nonexistent", 0, -1) == []

    def test_lrem(self, client):
        """Test LREM command"""
        client.rpush("list1", "value1", "value2", "value1", "value3", "value1")
        # Remove from head
        assert client.lrem("list1", 2, "value1") == 2
        assert client.lrange("list1", 0, -1) == ["value2", "value3", "value1"]

        # Remove from tail
        client.rpush("list2", "value1", "value2", "value1", "value3", "value1")
        assert client.lrem("list2", -2, "value1") == 2
        assert client.lrange("list2", 0, -1) == ["value1", "value2", "value3"]

        # Remove all occurrences
        client.rpush("list3", "value1", "value2", "value1", "value3", "value1")
        assert client.lrem("list3", 0, "value1") == 3
        assert client.lrange("list3", 0, -1) == ["value2", "value3"]

    def test_lset(self, client):
        """Test LSET command"""
        client.rpush("list1", "value1", "value2", "value3")
        assert client.lset("list1", 1, "new_value") == "OK"
        assert client.lrange("list1", 0, -1) == ["value1", "new_value", "value3"]

        # Set using negative index
        assert client.lset("list1", -1, "last_value") == "OK"
        assert client.lrange("list1", 0, -1) == ["value1", "new_value", "last_value"]

        # Try to set invalid index
        with pytest.raises(Exception):
            client.lset("list1", 5, "value")

        # Try to set in non-existing list
        with pytest.raises(Exception):
            client.lset("nonexistent", 0, "value")

    def test_ltrim(self, client):
        """Test LTRIM command"""
        client.rpush("list1", "value1", "value2", "value3", "value4", "value5")
        assert client.ltrim("list1", 1, 3) == "OK"
        assert client.lrange("list1", 0, -1) == ["value2", "value3", "value4"]

        # Trim with negative indices
        client.rpush("list2", "value1", "value2", "value3", "value4", "value5")
        assert client.ltrim("list2", 0, -2) == "OK"
        assert client.lrange("list2", 0, -1) == ["value1", "value2", "value3", "value4"]

        # Trim beyond list bounds
        assert client.ltrim("list1", 10, 20) == "OK"
        assert client.lrange("list1", 0, -1) == []

    def test_rpop(self, client):
        """Test RPOP command"""
        client.rpush("list1", "value1", "value2", "value3")
        # Single pop
        assert client.rpop("list1") == "value3"

        # Multiple pop
        client.rpush("list2", "value1", "value2", "value3")
        assert client.rpop("list2", 2) == ["value3", "value2"]

        # Pop from empty list
        assert client.rpop("nonexistent") is None

        # Pop more than available
        assert len(client.rpop("list2", 5)) == 1  # Only one element left

    def test_rpush(self, client):
        """Test RPUSH command"""
        assert client.rpush("list1", "value1") == 1
        assert client.rpush("list1", "value2", "value3") == 3
        assert client.lrange("list1", 0, -1) == ["value1", "value2", "value3"]

    def test_rpushx(self, client):
        """Test RPUSHX command"""
        # Push to non-existing list
        assert client.rpushx("list1", "value1") == 0
        assert client.llen("list1") == 0

        # Push to existing list
        client.rpush("list1", "value1")
        assert client.rpushx("list1", "value2", "value3") == 3
        assert client.lrange("list1", 0, -1) == ["value1", "value2", "value3"]

    def test_sort(self, client):
        """Test SORT command"""
        # Sort numbers
        client.rpush("list1", "3", "1", "2")
        assert client.sort("list1") == ["1", "2", "3"]

        # Sort with DESC
        assert client.sort("list1", desc=True) == ["3", "2", "1"]

        # Sort strings with ALPHA
        client.rpush("list2", "banana", "apple", "cherry")
        assert client.sort("list2", alpha=True) == ["apple", "banana", "cherry"]

        # Sort with STORE
        client.sort("list1", desc=True, store="sorted_list")
        assert client.lrange("sorted_list", 0, -1) == ["3", "2", "1"]


# todo to fix
@SkipTest
class TestSetCommands(BaseTest):
    def test_sadd(self, client):
        """Test SADD command"""
        # Add single member
        assert client.sadd("set1", "member1") == 1

        # Add multiple members
        assert client.sadd("set1", "member2", "member3") == 2

        # Add duplicate members
        assert client.sadd("set1", "member1", "member2", "member4") == 1

        # Verify set content
        members = client.smembers("set1")
        assert len(members) == 4
        assert all(m in members for m in ["member1", "member2", "member3", "member4"])

    def test_scard(self, client):
        """Test SCARD command"""
        assert client.scard("nonexistent") == 0

        client.sadd("set1", "member1", "member2", "member3")
        assert client.scard("set1") == 3

        client.sadd("set1", "member1")  # Duplicate
        assert client.scard("set1") == 3

    def test_sdiff(self, client):
        """Test SDIFF command"""
        client.sadd("set1", "a", "b", "c", "d")
        client.sadd("set2", "c", "d", "e")
        client.sadd("set3", "d", "e", "f")

        # Diff between two sets
        diff = client.sdiff("set1", "set2")
        assert set(diff) == {"a", "b"}

        # Diff between multiple sets
        diff = client.sdiff("set1", "set2", "set3")
        assert set(diff) == {"a", "b"}

        # Diff with nonexistent set
        diff = client.sdiff("set1", "nonexistent")
        assert set(diff) == {"a", "b", "c", "d"}

    def test_sdiffstore(self, client):
        """Test SDIFFSTORE command"""
        client.sadd("set1", "a", "b", "c", "d")
        client.sadd("set2", "c", "d", "e")

        # Store diff in new set
        assert client.sdiffstore("dest", "set1", "set2") == 2
        assert set(client.smembers("dest")) == {"a", "b"}

        # Store diff in existing set (overwrite)
        client.sadd("dest", "x", "y", "z")
        assert client.sdiffstore("dest", "set1", "set2") == 2
        assert set(client.smembers("dest")) == {"a", "b"}

    def test_sinter(self, client):
        """Test SINTER command"""
        client.sadd("set1", "a", "b", "c", "d")
        client.sadd("set2", "c", "d", "e")
        client.sadd("set3", "d", "e", "f")

        # Intersection between two sets
        inter = client.sinter("set1", "set2")
        assert set(inter) == {"c", "d"}

        # Intersection between multiple sets
        inter = client.sinter("set1", "set2", "set3")
        assert set(inter) == {"d"}

        # Intersection with nonexistent set
        assert client.sinter("set1", "nonexistent") == []

    def test_sintercard(self, client):
        """Test SINTERCARD command"""
        client.sadd("set1", "a", "b", "c", "d")
        client.sadd("set2", "c", "d", "e")
        client.sadd("set3", "d", "e", "f")

        # Count intersection between two sets
        assert client.sintercard(2, "set1", "set2") == 2

        # Count intersection between multiple sets
        assert client.sintercard(3, "set1", "set2", "set3") == 1

        # Test with limit
        assert client.sintercard(3, "set1", "set2", "set3", limit=1) == 1

    def test_sinterstore(self, client):
        """Test SINTERSTORE command"""
        client.sadd("set1", "a", "b", "c", "d")
        client.sadd("set2", "c", "d", "e")

        # Store intersection in new set
        assert client.sinterstore("dest", "set1", "set2") == 2
        assert set(client.smembers("dest")) == {"c", "d"}

        # Store intersection in existing set (overwrite)
        client.sadd("dest", "x", "y", "z")
        assert client.sinterstore("dest", "set1", "set2") == 2
        assert set(client.smembers("dest")) == {"c", "d"}

    def test_sismember(self, client):
        """Test SISMEMBER command"""
        client.sadd("set1", "member1", "member2")

        assert client.sismember("set1", "member1") == 1
        assert client.sismember("set1", "member2") == 1
        assert client.sismember("set1", "nonexistent") == 0
        assert client.sismember("nonexistent", "member1") == 0

    def test_smembers(self, client):
        """Test SMEMBERS command"""
        client.sadd("set1", "member1", "member2", "member3")

        members = client.smembers("set1")
        assert len(members) == 3
        assert all(m in members for m in ["member1", "member2", "member3"])

        assert client.smembers("nonexistent") == []

    def test_smismember(self, client):
        """Test SMISMEMBER command"""
        client.sadd("set1", "member1", "member2")

        result = client.smismember("set1", "member1", "member2", "nonexistent")
        assert result == [1, 1, 0]

        assert client.smismember("nonexistent", "member1") == [0]

    def test_smove(self, client):
        """Test SMOVE command"""
        client.sadd("source", "member1", "member2")
        client.sadd("dest", "member3")

        # Move existing member
        assert client.smove("source", "dest", "member1") == 1
        assert "member1" not in client.smembers("source")
        assert "member1" in client.smembers("dest")

        # Move non-existing member
        assert client.smove("source", "dest", "nonexistent") == 0

        # Move from non-existing source
        assert client.smove("nonexistent", "dest", "member") == 0

    def test_spop(self, client):
        """Test SPOP command"""
        client.sadd("set1", "member1", "member2", "member3")

        # Pop single member
        popped = client.spop("set1")
        assert popped in ["member1", "member2", "member3"]
        assert popped not in client.smembers("set1")

        # Pop multiple members
        client.sadd("set2", "member1", "member2", "member3", "member4")
        popped = client.spop("set2", 2)
        assert len(popped) == 2
        assert all(p not in client.smembers("set2") for p in popped)

        # Pop from empty set
        assert client.spop("nonexistent") is None
        assert client.spop("nonexistent", 2) == []

    def test_srandmember(self, client):
        """Test SRANDMEMBER command"""
        client.sadd("set1", "member1", "member2", "member3")

        # Get single random member
        member = client.srandmember("set1")
        assert member in ["member1", "member2", "member3"]
        assert len(client.smembers("set1")) == 3  # Set unchanged

        # Get multiple random members
        members = client.srandmember("set1", 2)
        assert len(members) == 2
        assert all(m in ["member1", "member2", "member3"] for m in members)

        # Get from empty set
        assert client.srandmember("nonexistent") is None
        assert client.srandmember("nonexistent", 2) == []

    def test_srem(self, client):
        """Test SREM command"""
        client.sadd("set1", "member1", "member2", "member3")

        # Remove single member
        assert client.srem("set1", "member1") == 1
        assert "member1" not in client.smembers("set1")

        # Remove multiple members
        assert client.srem("set1", "member2", "member3", "nonexistent") == 2
        assert client.smembers("set1") == []

        # Remove from empty set
        assert client.srem("nonexistent", "member1") == 0

    def test_sunion(self, client):
        """Test SUNION command"""
        client.sadd("set1", "a", "b", "c")
        client.sadd("set2", "c", "d", "e")
        client.sadd("set3", "e", "f", "g")

        # Union of two sets
        union = client.sunion("set1", "set2")
        assert set(union) == {"a", "b", "c", "d", "e"}

        # Union of multiple sets
        union = client.sunion("set1", "set2", "set3")
        assert set(union) == {"a", "b", "c", "d", "e", "f", "g"}

        # Union with nonexistent set
        union = client.sunion("set1", "nonexistent")
        assert set(union) == {"a", "b", "c"}


# todo to fix
@SkipTest
class TestZSetCommands(BaseTest):
    def test_zadd(self, client):
        """Test ZADD command"""
        # Add single member
        assert client.zadd("zset1", {"member1": 1.0}) == 1

        # Add multiple members
        assert client.zadd("zset1", {
            "member2": 2.0,
            "member3": 3.0
        }) == 2

        # Update existing member
        assert client.zadd("zset1", {"member1": 1.5}) == 0
        assert client.zscore("zset1", "member1") == "1.5"

    def test_zcard(self, client):
        """Test ZCARD command"""
        assert client.zcard("zset1") == 0
        client.zadd("zset1", {"member1": 1.0, "member2": 2.0})
        assert client.zcard("zset1") == 2

    def test_zcount(self, client):
        """Test ZCOUNT command"""
        client.zadd("zset1", {
            "member1": 1.0,
            "member2": 2.0,
            "member3": 3.0
        })
        assert client.zcount("zset1", 2.0, 3.0) == 2
        assert client.zcount("zset1", 0, 5) == 3

    def test_zdiff(self, client):
        """Test ZDIFF command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        client.zadd("zset2", {"b": 2, "c": 3, "d": 4})
        result = client.zdiff(2, "zset1", "zset2")
        assert "a" in result

    def test_zincrby(self, client):
        """Test ZINCRBY command"""
        client.zadd("zset1", {"member1": 1.0})
        assert client.zincrby("zset1", 2.5, "member1") == "3.5"
        assert client.zincrby("zset1", -1.5, "member1") == "2"

    def test_zinter(self, client):
        """Test ZINTER command"""
        client.zadd("zset1", {"a": 1, "b": 2})
        client.zadd("zset2", {"b": 2, "c": 3})
        result = client.zinter(2, "zset1", "zset2")
        assert "b" in result

    def test_zintercard(self, client):
        """Test ZINTERCARD command"""
        client.zadd("zset1", {"a": 1, "b": 2})
        client.zadd("zset2", {"b": 2, "c": 3})
        assert client.zintercard(2, "zset1", "zset2") == 1
        assert client.zintercard(2, "zset1", "zset2", limit=1) == 1

    def test_zinterstore(self, client):
        """Test ZINTERSTORE command"""
        client.zadd("zset1", {"a": 1, "b": 2})
        client.zadd("zset2", {"b": 2, "c": 3})
        assert client.zinterstore("dest", 2, "zset1", "zset2") == 1
        assert "b" in client.zrange("dest", 0, -1)

    def test_zmpop(self, client):
        """Test ZMPOP command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        result = client.zmpop(1, "zset1", min=True)
        assert result[0] == "zset1"
        assert "a" in result[1]

        result = client.zmpop(1, "zset1", min=False, count=2)
        assert len(result[1]) == 4  # Two members with scores

    def test_zmscore(self, client):
        """Test ZMSCORE command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        scores = client.zmscore("zset1", "a", "b", "nonexistent")
        assert scores == ["1", "2", None]

    def test_zpopmax(self, client):
        """Test ZPOPMAX command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        assert client.zpopmax("zset1") == ["c", 3]
        assert client.zpopmax("zset1", 2) == ["b", 2, "a", 1]

    def test_zpopmin(self, client):
        """Test ZPOPMIN command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        assert client.zpopmin("zset1") == ["a", "1"]
        assert client.zpopmin("zset1", 2) == ["b", "2", "c", "3"]

    def test_zrandmember(self, client):
        """Test ZRANDMEMBER command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        member = client.zrandmember("zset1")
        assert member in ["a", "b", "c"]

        members = client.zrandmember("zset1", 2)
        assert len(members) == 2

        result = client.zrandmember("zset1", 2, withscores=True)
        assert len(result) == 4  # [member1, score1, member2, score2]

    def test_zrange(self, client):
        """Test ZRANGE command with various options"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})

        # Basic range
        assert client.zrange("zset1", 0, -1) == ["a", "b", "c"]

        # With scores
        result = client.zrange("zset1", 0, -1, withscores=True)
        assert result == ["a", "1", "b", "2", "c", "3"]

        # By score
        result = client.zrange("zset1", "2", "3", byscore=True)
        assert result == ["b", "c"]

        # By lex
        result = client.zrange("zset1", "[b", "[c", bylex=True)
        assert result == ["b", "c"]

        # Reverse order
        result = client.zrange("zset1", 0, -1, rev=True)
        assert result == ["c", "b", "a"]

        # With limit
        result = client.zrange("zset1", 0, -1, offset=1, count=1)
        assert result == ["b"]

    def test_zrangestore(self, client):
        """Test ZRANGESTORE command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})

        # Basic range store
        assert client.zrangestore("dest", "zset1", "0", "2") == 2
        assert client.zrange("dest", 0, -1) == ["a", "b"]

        # Store with BYSCORE
        assert client.zrangestore("dest", "zset1", "2", "3", byscore=True) == 2
        assert client.zrange("dest", 0, -1) == ["b", "c"]

    def test_zrank(self, client):
        """Test ZRANK command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        assert client.zrank("zset1", "b") == 1
        assert client.zrank("zset1", "nonexistent") is None

    def test_zrem(self, client):
        """Test ZREM command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        assert client.zrem("zset1", "a", "nonexistent") == 1
        assert client.zcard("zset1") == 2

    def test_zremrangebyscore(self, client):
        """Test ZREMRANGEBYSCORE command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        assert client.zremrangebyscore("zset1", 2, 3) == 2
        assert client.zcard("zset1") == 1

    def test_zrevrank(self, client):
        """Test ZREVRANK command"""
        client.zadd("zset1", {"a": 1, "b": 2, "c": 3})
        assert client.zrevrank("zset1", "b") == 1
        assert client.zrevrank("zset1", "nonexistent") is None

    def test_zscore(self, client):
        """Test ZSCORE command"""
        client.zadd("zset1", {"a": 1.5})
        assert client.zscore("zset1", "a") == 1.5
        assert client.zscore("zset1", "nonexistent") is None

    def test_zunion(self, client):
        """Test ZUNION command"""
        client.zadd("zset1", {"a": 1, "b": 2})
        client.zadd("zset2", {"b": 2, "c": 3})
        result = client.zunion(2, "zset1", "zset2")
        assert set(result) == {"a", "b", "c"}

    def test_zunionstore(self, client):
        """Test ZUNIONSTORE command"""
        client.zadd("zset1", {"a": 1, "b": 2})
        client.zadd("zset2", {"b": 2, "c": 3})
        assert client.zunionstore("dest", 2, "zset1", "zset2") == 3
        assert set(client.zrange("dest", 0, -1)) == {"a", "b", "c"}