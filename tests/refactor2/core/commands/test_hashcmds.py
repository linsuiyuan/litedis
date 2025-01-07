import pytest

from refactor2.core.command.base import CommandContext
from refactor2.core.command.hashcmds import (
    HDelCommand,
    HExistsCommand,
    HGetCommand,
    HGetAllCommand,
    HIncrByCommand, HIncrByFloatCommand, HKeysCommand, HLenCommand, HSetCommand, HSetNXCommand, HMGetCommand,
    HValsCommand, HStrLenCommand, HScanCommand,
)
from refactor2.core.persistence.ldb import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db)


class TestHDelCommand:
    def test_hdel_single_field(self, ctx):
        # Setup
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        # Execute
        cmd = HDelCommand(["hdel", "hash1", "field1"])
        result = cmd.execute(ctx)

        # Verify
        assert result == 1
        assert ctx.db.get("hash1") == {"field2": "value2"}

    def test_hdel_multiple_fields(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2", "field3": "value3"})

        cmd = HDelCommand(["hdel", "hash1", "field1", "field2"])
        result = cmd.execute(ctx)

        assert result == 2
        assert ctx.db.get("hash1") == {"field3": "value3"}

    def test_hdel_nonexistent_key(self, ctx):
        cmd = HDelCommand(["hdel", "nonexistent", "field1"])
        result = cmd.execute(ctx)

        assert result == 0

    def test_hdel_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        cmd = HDelCommand(["hdel", "hash1", "nonexistent"])
        result = cmd.execute(ctx)

        assert result == 0
        assert ctx.db.get("hash1") == {"field1": "value1"}

    def test_hdel_all_fields_removes_key(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        cmd = HDelCommand(["hdel", "hash1", "field1"])
        result = cmd.execute(ctx)

        assert result == 1
        assert not ctx.db.exists("hash1")

    def test_hdel_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HDelCommand(["hdel", "string1", "field1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHExistsCommand:
    def test_hexists_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        cmd = HExistsCommand(["hexists", "hash1", "field1"])
        result = cmd.execute(ctx)

        assert result == 1

    def test_hexists_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        cmd = HExistsCommand(["hexists", "hash1", "nonexistent"])
        result = cmd.execute(ctx)

        assert result == 0

    def test_hexists_nonexistent_key(self, ctx):
        cmd = HExistsCommand(["hexists", "nonexistent", "field1"])
        result = cmd.execute(ctx)

        assert result == 0

    def test_hexists_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HExistsCommand(["hexists", "string1", "field1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHGetCommand:
    def test_hget_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        cmd = HGetCommand(["hget", "hash1", "field1"])
        result = cmd.execute(ctx)

        assert result == "value1"

    def test_hget_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        cmd = HGetCommand(["hget", "hash1", "nonexistent"])
        result = cmd.execute(ctx)

        assert result is None

    def test_hget_nonexistent_key(self, ctx):
        cmd = HGetCommand(["hget", "nonexistent", "field1"])
        result = cmd.execute(ctx)

        assert result is None

    def test_hget_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HGetCommand(["hget", "string1", "field1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHGetAllCommand:
    def test_hgetall_existing_hash(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        cmd = HGetAllCommand(["hgetall", "hash1"])
        result = cmd.execute(ctx)

        assert len(result) == 4
        assert set(result[::2]) == {"field1", "field2"}  # Fields
        assert set(result[1::2]) == {"value1", "value2"}  # Values

    def test_hgetall_nonexistent_key(self, ctx):
        cmd = HGetAllCommand(["hgetall", "nonexistent"])
        result = cmd.execute(ctx)

        assert result == []

    def test_hgetall_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HGetAllCommand(["hgetall", "string1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHIncrByCommand:
    def test_hincrby_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "5"})

        cmd = HIncrByCommand(["hincrby", "hash1", "field1", "3"])
        result = cmd.execute(ctx)

        assert result == 8
        assert ctx.db.get("hash1")["field1"] == "8"

    def test_hincrby_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {})

        cmd = HIncrByCommand(["hincrby", "hash1", "field1", "3"])
        result = cmd.execute(ctx)

        assert result == 3
        assert ctx.db.get("hash1")["field1"] == "3"

    def test_hincrby_nonexistent_key(self, ctx):
        cmd = HIncrByCommand(["hincrby", "hash1", "field1", "3"])
        result = cmd.execute(ctx)

        assert result == 3
        assert ctx.db.get("hash1")["field1"] == "3"

    def test_hincrby_negative_increment(self, ctx):
        ctx.db.set("hash1", {"field1": "5"})

        cmd = HIncrByCommand(["hincrby", "hash1", "field1", "-3"])
        result = cmd.execute(ctx)

        assert result == 2
        assert ctx.db.get("hash1")["field1"] == "2"

    def test_hincrby_invalid_value(self, ctx):
        ctx.db.set("hash1", {"field1": "not_a_number"})

        cmd = HIncrByCommand(["hincrby", "hash1", "field1", "3"])
        with pytest.raises(ValueError, match="value is not an integer"):
            cmd.execute(ctx)

    def test_hincrby_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HIncrByCommand(["hincrby", "string1", "field1", "3"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHIncrByFloatCommand:
    def test_hincrbyfloat_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "5.5"})

        cmd = HIncrByFloatCommand(["hincrbyfloat", "hash1", "field1", "3.3"])
        result = cmd.execute(ctx)

        assert result == "8.8"
        assert ctx.db.get("hash1")["field1"] == "8.8"

    def test_hincrbyfloat_whole_number_result(self, ctx):
        ctx.db.set("hash1", {"field1": "5.5"})

        cmd = HIncrByFloatCommand(["hincrbyfloat", "hash1", "field1", "4.5"])
        result = cmd.execute(ctx)

        assert result == "10"  # No decimal point for whole numbers
        assert ctx.db.get("hash1")["field1"] == "10"

    def test_hincrbyfloat_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {})

        cmd = HIncrByFloatCommand(["hincrbyfloat", "hash1", "field1", "3.14"])
        result = cmd.execute(ctx)

        assert result == "3.14"
        assert ctx.db.get("hash1")["field1"] == "3.14"

    def test_hincrbyfloat_nonexistent_key(self, ctx):
        cmd = HIncrByFloatCommand(["hincrbyfloat", "hash1", "field1", "3.14"])
        result = cmd.execute(ctx)

        assert result == "3.14"
        assert ctx.db.get("hash1")["field1"] == "3.14"

    def test_hincrbyfloat_negative_increment(self, ctx):
        ctx.db.set("hash1", {"field1": "5.5"})

        cmd = HIncrByFloatCommand(["hincrbyfloat", "hash1", "field1", "-2.2"])
        result = cmd.execute(ctx)

        assert result == "3.3"
        assert ctx.db.get("hash1")["field1"] == "3.3"

    def test_hincrbyfloat_invalid_value(self, ctx):
        ctx.db.set("hash1", {"field1": "not_a_number"})

        cmd = HIncrByFloatCommand(["hincrbyfloat", "hash1", "field1", "3.14"])
        with pytest.raises(ValueError, match="value is not a float"):
            cmd.execute(ctx)

    def test_hincrbyfloat_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HIncrByFloatCommand(["hincrbyfloat", "string1", "field1", "3.14"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHKeysCommand:
    def test_hkeys_existing_hash(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        cmd = HKeysCommand(["hkeys", "hash1"])
        result = cmd.execute(ctx)

        assert set(result) == {"field1", "field2"}

    def test_hkeys_empty_hash(self, ctx):
        ctx.db.set("hash1", {})

        cmd = HKeysCommand(["hkeys", "hash1"])
        result = cmd.execute(ctx)

        assert result == []

    def test_hkeys_nonexistent_key(self, ctx):
        cmd = HKeysCommand(["hkeys", "nonexistent"])
        result = cmd.execute(ctx)

        assert result == []

    def test_hkeys_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HKeysCommand(["hkeys", "string1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHLenCommand:
    def test_hlen_existing_hash(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        cmd = HLenCommand(["hlen", "hash1"])
        result = cmd.execute(ctx)

        assert result == 2

    def test_hlen_empty_hash(self, ctx):
        ctx.db.set("hash1", {})

        cmd = HLenCommand(["hlen", "hash1"])
        result = cmd.execute(ctx)

        assert result == 0

    def test_hlen_nonexistent_key(self, ctx):
        cmd = HLenCommand(["hlen", "nonexistent"])
        result = cmd.execute(ctx)

        assert result == 0

    def test_hlen_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HLenCommand(["hlen", "string1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHSetCommand:
    def test_hset_new_field(self, ctx):
        ctx.db.set("hash1", {"existing": "value"})

        cmd = HSetCommand(["hset", "hash1", "field1", "value1"])
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get("hash1") == {"existing": "value", "field1": "value1"}

    def test_hset_multiple_fields(self, ctx):
        ctx.db.set("hash1", {"existing": "value"})

        cmd = HSetCommand(["hset", "hash1", "field1", "value1", "field2", "value2"])
        result = cmd.execute(ctx)

        assert result == 2
        assert ctx.db.get("hash1") == {
            "existing": "value",
            "field1": "value1",
            "field2": "value2"
        }

    def test_hset_update_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "old_value"})

        cmd = HSetCommand(["hset", "hash1", "field1", "new_value"])
        result = cmd.execute(ctx)

        assert result == 0
        assert ctx.db.get("hash1") == {"field1": "new_value"}

    def test_hset_nonexistent_key(self, ctx):
        cmd = HSetCommand(["hset", "hash1", "field1", "value1"])
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get("hash1") == {"field1": "value1"}

    def test_hset_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HSetCommand(["hset", "string1", "field1", "value1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHSetNXCommand:
    def test_hsetnx_new_field(self, ctx):
        ctx.db.set("hash1", {"existing": "value"})

        cmd = HSetNXCommand(["hsetnx", "hash1", "field1", "value1"])
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get("hash1") == {"existing": "value", "field1": "value1"}

    def test_hsetnx_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "old_value"})

        cmd = HSetNXCommand(["hsetnx", "hash1", "field1", "new_value"])
        result = cmd.execute(ctx)

        assert result == 0
        assert ctx.db.get("hash1") == {"field1": "old_value"}

    def test_hsetnx_nonexistent_key(self, ctx):
        cmd = HSetNXCommand(["hsetnx", "hash1", "field1", "value1"])
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get("hash1") == {"field1": "value1"}

    def test_hsetnx_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HSetNXCommand(["hsetnx", "string1", "field1", "value1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHMGetCommand:
    def test_hmget_existing_fields(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2", "field3": "value3"})

        cmd = HMGetCommand(["hmget", "hash1", "field1", "field2"])
        result = cmd.execute(ctx)

        assert result == ["value1", "value2"]

    def test_hmget_mixed_existing_nonexisting(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field3": "value3"})

        cmd = HMGetCommand(["hmget", "hash1", "field1", "field2", "field3"])
        result = cmd.execute(ctx)

        assert result == ["value1", None, "value3"]

    def test_hmget_nonexistent_key(self, ctx):
        cmd = HMGetCommand(["hmget", "nonexistent", "field1", "field2"])
        result = cmd.execute(ctx)

        assert result == [None, None]

    def test_hmget_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HMGetCommand(["hmget", "string1", "field1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHValsCommand:
    def test_hvals_existing_hash(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        cmd = HValsCommand(["hvals", "hash1"])
        result = cmd.execute(ctx)

        assert set(result) == {"value1", "value2"}

    def test_hvals_empty_hash(self, ctx):
        ctx.db.set("hash1", {})

        cmd = HValsCommand(["hvals", "hash1"])
        result = cmd.execute(ctx)

        assert result == []

    def test_hvals_nonexistent_key(self, ctx):
        cmd = HValsCommand(["hvals", "nonexistent"])
        result = cmd.execute(ctx)

        assert result == []

    def test_hvals_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HValsCommand(["hvals", "string1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHStrLenCommand:
    def test_hstrlen_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "hello", "field2": "world!"})

        cmd = HStrLenCommand(["hstrlen", "hash1", "field1"])
        result = cmd.execute(ctx)

        assert result == 5

    def test_hstrlen_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {"field1": "hello"})

        cmd = HStrLenCommand(["hstrlen", "hash1", "nonexistent"])
        result = cmd.execute(ctx)

        assert result == 0

    def test_hstrlen_nonexistent_key(self, ctx):
        cmd = HStrLenCommand(["hstrlen", "nonexistent", "field1"])
        result = cmd.execute(ctx)

        assert result == 0

    def test_hstrlen_numeric_value(self, ctx):
        ctx.db.set("hash1", {"field1": "12345"})

        cmd = HStrLenCommand(["hstrlen", "hash1", "field1"])
        result = cmd.execute(ctx)

        assert result == 5

    def test_hstrlen_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HStrLenCommand(["hstrlen", "string1", "field1"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHScanCommand:
    def test_hscan_basic(self, ctx):
        ctx.db.set("hash1", {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        })

        cmd = HScanCommand(["hscan", "hash1", "0"])
        result = cmd.execute(ctx)

        assert result[0] == 0  # cursor
        assert len(result[1]) == 6  # 3 fields * 2 (field + value)
        # Convert flat list to dict for easier comparison
        result_dict = dict(zip(result[1][::2], result[1][1::2]))
        assert result_dict == {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }

    def test_hscan_with_pattern(self, ctx):
        ctx.db.set("hash1", {
            "field1": "value1",
            "field2": "value2",
            "test1": "value3"
        })

        cmd = HScanCommand(["hscan", "hash1", "0", "match", "field*"])
        result = cmd.execute(ctx)

        assert result[0] == 0
        # Convert flat list to dict for easier comparison
        result_dict = dict(zip(result[1][::2], result[1][1::2]))
        assert result_dict == {
            "field1": "value1",
            "field2": "value2"
        }

    def test_hscan_with_count(self, ctx):
        ctx.db.set("hash1", {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        })

        cmd = HScanCommand(["hscan", "hash1", "0", "count", "2"])
        result = cmd.execute(ctx)

        # In this implementation, count is just a hint and all results are returned
        assert result[0] == 0
        assert len(result[1]) == 6

    def test_hscan_nonexistent_key(self, ctx):
        cmd = HScanCommand(["hscan", "nonexistent", "0"])
        result = cmd.execute(ctx)

        assert result == [0, []]

    def test_hscan_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        cmd = HScanCommand(["hscan", "string1", "0"])
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)

    def test_hscan_invalid_cursor(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        with pytest.raises(ValueError, match="cursor must be an integer"):
            HScanCommand(["hscan", "hash1", "invalid"])
