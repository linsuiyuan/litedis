import time

import pytest

from litedis.core.command.sortedset import SortedSet
from litedis.core.persistence import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test_db")


def test_set_and_get(db):
    db.set("key1", "value1")
    assert db.get("key1") == "value1"
    assert db.get("nonexistent") is None


def test_set_type_checking(db):
    db.set("str_key", "string")
    db.set("list_key", [1, 2, 3])
    db.set("dict_key", {"a": 1})
    db.set("set_key", {1, 2, 3})

    # Test type mismatch
    with pytest.raises(TypeError):
        db.set("str_key", [1, 2, 3])

    # Test unsupported type
    with pytest.raises(TypeError):
        db.set("invalid", 42)  # noqa


def test_get(db):
    # Test basic get operation
    db.set("key1", "value1")
    assert db.get("key1") == "value1"

    # Test non-existent key
    assert db.get("nonexistent") is None


def test_get_str(db):
    db.set("str_key", "string")
    assert db.get_str("str_key") == "string"

    db.set("not str key", [])
    with pytest.raises(TypeError):
        db.get_str("not str key")


def test_get_dict(db):
    db.set("dict_key", {"a": 1, "b": 2})
    assert db.get_dict("dict_key") == {"a": 1, "b": 2}

    db.set("not dict key", [])
    with pytest.raises(TypeError):
        db.get_dict("not dict key")


def test_get_list(db):
    db.set("list_key", [1, 2, 3])
    assert db.get_list("list_key") == [1, 2, 3]

    db.set("not list key", {})
    with pytest.raises(TypeError):
        db.get_list("not list key")


def test_get_set(db):
    db.set("set_key", {1, 2, 3})
    assert db.get_set("set_key") == {1, 2, 3}

    db.set("not set key", [])
    with pytest.raises(TypeError):
        db.get_set("not set key")


def test_get_zset(db):
    db.set("zset_key", SortedSet({"member1": 1., "member2": 2.}))
    assert type(db.get_zset("zset_key")) == SortedSet

    db.set("not zset key", [])
    with pytest.raises(TypeError):
        db.get_zset("not zset key")


def test_get_with_expiration(db):
    # Test get with future expiration
    db.set("future_key", "value")
    future_time = int(time.time() * 1000) + 10000  # 10 se
    db.set_expiration("future_key", future_time)
    assert db.get("future_key") == "value"

    # Test get with expired key
    db.set("expired_key", "value")
    past_time = int(time.time() * 1000) - 1000  # 1 second ago
    db.set_expiration("expired_key", past_time)
    assert db.get("expired_key") is None

    # Verify expired key is deleted
    assert not db.exists("expired_key")
    assert "expired_key" not in db._expirations


def test_exists(db):
    db.set("key1", "value1")
    assert db.exists("key1") is True
    assert db.exists("nonexistent") is False


def test_delete(db):
    db.set("key1", "value1")
    assert db.delete("key1") == 1
    assert db.exists("key1") is False
    assert db.delete("nonexistent") == 0


def test_keys(db):
    test_data = {
        "key1": "value1",
        "key2": ["list", "value"],
        "key3": {"dict": "value"}
    }

    for k, v in test_data.items():
        db.set(k, v)

    assert set(db.keys()) == set(test_data.keys())


def test_set_expiration(db):
    db.set("key1", "value1")
    assert db.set_expiration("key1", 100) == 1
    assert db.set_expiration("nonexistent", 100) == 0


def test_get_expiration(db):
    # Set key-value and expiration
    db.set("key1", "value1")
    db.set_expiration("key1", 1000)
    assert db.get_expiration("key1") == 1000

    # Test non-existent key
    assert db.get_expiration("nonexistent") == -2

    # Test key exists but without expiration
    db.set("key2", "value2")
    assert db.get_expiration("key2") == -1


def test_exists_expiration(db):
    db.set("key1", "value1")
    db.set_expiration("key1", 100)
    assert db.exists_expiration("key1") is True
    assert db.exists_expiration("nonexistent") is False


def test_delete_expiration(db):
    db.set("key1", "value1")
    db.set_expiration("key1", 100)
    assert db.delete_expiration("key1") == 1
    assert db.delete_expiration("nonexistent") == 0
    assert db.exists_expiration("key1") is False


def test_get_type(db):
    type_tests = {
        "string_key": ("string_value", "string"),
        "list_key": (["list", "value"], "list"),
        "dict_key": ({"dict": "value"}, "hash"),
        "set_key": ({1, 2, 3}, "set"),
        "zset_key": (SortedSet({"member1": 1., "member2": 2.}), "zset"),
    }

    for key, (value, expected_type) in type_tests.items():
        db.set(key, value)
        assert db.get_type(key) == expected_type

    assert db.get_type("nonexistent") == "none"
