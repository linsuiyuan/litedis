import pytest

from refactor2.core.command.base import CommandContext
from refactor2.core.command.hashcmds import (
    HDelCommand,
    HExistsCommand,
    HGetCommand,
    HGetAllCommand,
    HIncrByCommand,
    HIncrByFloatCommand,
    HKeysCommand,
    HLenCommand,
    HSetCommand,
    HSetNXCommand,
    HMGetCommand,
    HValsCommand,
    HStrLenCommand,
    HScanCommand,
)
from refactor2.core.persistence.ldb import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db, [])


class TestHDelCommand:
    def test_hdel_single_field(self, ctx):
        # Setup
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        # Execute
        ctx.cmdtokens = ["hdel", "hash1", "field1"]
        cmd = HDelCommand()
        result = cmd.execute(ctx)

        # Verify
        assert result == 1
        assert ctx.db.get("hash1") == {"field2": "value2"}

    def test_hdel_multiple_fields(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2", "field3": "value3"})

        ctx.cmdtokens = ["hdel", "hash1", "field1", "field2"]
        cmd = HDelCommand()
        result = cmd.execute(ctx)

        assert result == 2
        assert ctx.db.get("hash1") == {"field3": "value3"}

    def test_hdel_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hdel", "nonexistent", "field1"]
        cmd = HDelCommand()
        result = cmd.execute(ctx)

        assert result == 0

    def test_hdel_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        ctx.cmdtokens = ["hdel", "hash1", "nonexistent"]
        cmd = HDelCommand()
        result = cmd.execute(ctx)

        assert result == 0
        assert ctx.db.get("hash1") == {"field1": "value1"}

    def test_hdel_all_fields_removes_key(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        ctx.cmdtokens = ["hdel", "hash1", "field1"]
        cmd = HDelCommand()
        result = cmd.execute(ctx)

        assert result == 1
        assert not ctx.db.exists("hash1")

    def test_hdel_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hdel", "string1", "field1"]
        cmd = HDelCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHExistsCommand:
    def test_hexists_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        ctx.cmdtokens = ["hexists", "hash1", "field1"]
        cmd = HExistsCommand()
        result = cmd.execute(ctx)

        assert result == 1

    def test_hexists_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        ctx.cmdtokens = ["hexists", "hash1", "nonexistent"]
        cmd = HExistsCommand()
        result = cmd.execute(ctx)

        assert result == 0

    def test_hexists_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hexists", "nonexistent", "field1"]
        cmd = HExistsCommand()
        result = cmd.execute(ctx)

        assert result == 0

    def test_hexists_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hexists", "string1", "field1"]
        cmd = HExistsCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHGetCommand:
    def test_hget_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        ctx.cmdtokens = ["hget", "hash1", "field1"]
        cmd = HGetCommand()
        result = cmd.execute(ctx)

        assert result == "value1"

    def test_hget_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {"field1": "value1"})

        ctx.cmdtokens = ["hget", "hash1", "nonexistent"]
        cmd = HGetCommand()
        result = cmd.execute(ctx)

        assert result is None

    def test_hget_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hget", "nonexistent", "field1"]
        cmd = HGetCommand()
        result = cmd.execute(ctx)

        assert result is None

    def test_hget_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hget", "string1", "field1"]
        cmd = HGetCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHGetAllCommand:
    def test_hgetall_existing_hash(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        ctx.cmdtokens = ["hgetall", "hash1"]
        cmd = HGetAllCommand()
        result = cmd.execute(ctx)

        assert len(result) == 4
        assert set(result[::2]) == {"field1", "field2"}  # Fields
        assert set(result[1::2]) == {"value1", "value2"}  # Values

    def test_hgetall_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hgetall", "nonexistent"]
        cmd = HGetAllCommand()
        result = cmd.execute(ctx)

        assert result == []

    def test_hgetall_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hgetall", "string1"]
        cmd = HGetAllCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHIncrByCommand:
    def test_hincrby_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "5"})

        ctx.cmdtokens = ["hincrby", "hash1", "field1", "3"]
        cmd = HIncrByCommand()
        result = cmd.execute(ctx)

        assert result == 8
        assert ctx.db.get("hash1")["field1"] == "8"

    def test_hincrby_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {})

        ctx.cmdtokens = ["hincrby", "hash1", "field1", "3"]
        cmd = HIncrByCommand()
        result = cmd.execute(ctx)

        assert result == 3
        assert ctx.db.get("hash1")["field1"] == "3"

    def test_hincrby_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hincrby", "hash1", "field1", "3"]
        cmd = HIncrByCommand()
        result = cmd.execute(ctx)

        assert result == 3
        assert ctx.db.get("hash1")["field1"] == "3"

    def test_hincrby_negative_increment(self, ctx):
        ctx.db.set("hash1", {"field1": "5"})

        ctx.cmdtokens = ["hincrby", "hash1", "field1", "-3"]
        cmd = HIncrByCommand()
        result = cmd.execute(ctx)

        assert result == 2
        assert ctx.db.get("hash1")["field1"] == "2"

    def test_hincrby_invalid_value(self, ctx):
        ctx.db.set("hash1", {"field1": "not_a_number"})

        ctx.cmdtokens = ["hincrby", "hash1", "field1", "3"]
        cmd = HIncrByCommand()
        with pytest.raises(ValueError, match="value is not an integer"):
            cmd.execute(ctx)

    def test_hincrby_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hincrby", "string1", "field1", "3"]
        cmd = HIncrByCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHIncrByFloatCommand:
    def test_hincrbyfloat_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "5.5"})

        ctx.cmdtokens = ["hincrbyfloat", "hash1", "field1", "3.3"]
        cmd = HIncrByFloatCommand()
        result = cmd.execute(ctx)

        assert result == "8.8"
        assert ctx.db.get("hash1")["field1"] == "8.8"

    def test_hincrbyfloat_whole_number_result(self, ctx):
        ctx.db.set("hash1", {"field1": "5.5"})

        ctx.cmdtokens = ["hincrbyfloat", "hash1", "field1", "4.5"]
        cmd = HIncrByFloatCommand()
        result = cmd.execute(ctx)

        assert result == "10"  # No decimal point for whole numbers
        assert ctx.db.get("hash1")["field1"] == "10"

    def test_hincrbyfloat_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {})

        ctx.cmdtokens = ["hincrbyfloat", "hash1", "field1", "3.14"]
        cmd = HIncrByFloatCommand()
        result = cmd.execute(ctx)

        assert result == "3.14"
        assert ctx.db.get("hash1")["field1"] == "3.14"

    def test_hincrbyfloat_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hincrbyfloat", "hash1", "field1", "3.14"]
        cmd = HIncrByFloatCommand()
        result = cmd.execute(ctx)

        assert result == "3.14"
        assert ctx.db.get("hash1")["field1"] == "3.14"

    def test_hincrbyfloat_negative_increment(self, ctx):
        ctx.db.set("hash1", {"field1": "5.5"})

        ctx.cmdtokens = ["hincrbyfloat", "hash1", "field1", "-2.2"]
        cmd = HIncrByFloatCommand()
        result = cmd.execute(ctx)

        assert result == "3.3"
        assert ctx.db.get("hash1")["field1"] == "3.3"

    def test_hincrbyfloat_invalid_value(self, ctx):
        ctx.db.set("hash1", {"field1": "not_a_number"})

        ctx.cmdtokens = ["hincrbyfloat", "hash1", "field1", "3.14"]
        cmd = HIncrByFloatCommand()
        with pytest.raises(ValueError, match="value is not a float"):
            cmd.execute(ctx)

    def test_hincrbyfloat_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hincrbyfloat", "string1", "field1", "3.14"]
        cmd = HIncrByFloatCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHKeysCommand:
    def test_hkeys_existing_hash(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        ctx.cmdtokens = ["hkeys", "hash1"]
        cmd = HKeysCommand()
        result = cmd.execute(ctx)

        assert set(result) == {"field1", "field2"}

    def test_hkeys_empty_hash(self, ctx):
        ctx.db.set("hash1", {})

        ctx.cmdtokens = ["hkeys", "hash1"]
        cmd = HKeysCommand()
        result = cmd.execute(ctx)

        assert result == []

    def test_hkeys_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hkeys", "nonexistent"]
        cmd = HKeysCommand()
        result = cmd.execute(ctx)

        assert result == []

    def test_hkeys_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hkeys", "string1"]
        cmd = HKeysCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHLenCommand:
    def test_hlen_existing_hash(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        ctx.cmdtokens = ["hlen", "hash1"]
        cmd = HLenCommand()
        result = cmd.execute(ctx)

        assert result == 2

    def test_hlen_empty_hash(self, ctx):
        ctx.db.set("hash1", {})

        ctx.cmdtokens = ["hlen", "hash1"]
        cmd = HLenCommand()
        result = cmd.execute(ctx)

        assert result == 0

    def test_hlen_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hlen", "nonexistent"]
        cmd = HLenCommand()
        result = cmd.execute(ctx)

        assert result == 0

    def test_hlen_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hlen", "string1"]
        cmd = HLenCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHSetCommand:
    def test_hset_new_field(self, ctx):
        ctx.db.set("hash1", {"existing": "value"})

        ctx.cmdtokens = ["hset", "hash1", "field1", "value1"]
        cmd = HSetCommand()
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get("hash1") == {"existing": "value", "field1": "value1"}

    def test_hset_multiple_fields(self, ctx):
        ctx.db.set("hash1", {"existing": "value"})

        ctx.cmdtokens = ["hset", "hash1", "field1", "value1", "field2", "value2"]
        cmd = HSetCommand()
        result = cmd.execute(ctx)

        assert result == 2
        assert ctx.db.get("hash1") == {
            "existing": "value",
            "field1": "value1",
            "field2": "value2"
        }

    def test_hset_update_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "old_value"})

        ctx.cmdtokens = ["hset", "hash1", "field1", "new_value"]
        cmd = HSetCommand()
        result = cmd.execute(ctx)

        assert result == 0
        assert ctx.db.get("hash1") == {"field1": "new_value"}

    def test_hset_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hset", "hash1", "field1", "value1"]
        cmd = HSetCommand()
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get("hash1") == {"field1": "value1"}

    def test_hset_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hset", "string1", "field1", "value1"]
        cmd = HSetCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHSetNXCommand:
    def test_hsetnx_new_field(self, ctx):
        ctx.db.set("hash1", {"existing": "value"})

        ctx.cmdtokens = ["hsetnx", "hash1", "field1", "value1"]
        cmd = HSetNXCommand()
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get("hash1") == {"existing": "value", "field1": "value1"}

    def test_hsetnx_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "old_value"})

        ctx.cmdtokens = ["hsetnx", "hash1", "field1", "new_value"]
        cmd = HSetNXCommand()
        result = cmd.execute(ctx)

        assert result == 0
        assert ctx.db.get("hash1") == {"field1": "old_value"}

    def test_hsetnx_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hsetnx", "hash1", "field1", "value1"]
        cmd = HSetNXCommand()
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get("hash1") == {"field1": "value1"}

    def test_hsetnx_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hsetnx", "string1", "field1", "value1"]
        cmd = HSetNXCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHMGetCommand:
    def test_hmget_existing_fields(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2", "field3": "value3"})

        ctx.cmdtokens = ["hmget", "hash1", "field1", "field2"]
        cmd = HMGetCommand()
        result = cmd.execute(ctx)

        assert result == ["value1", "value2"]

    def test_hmget_mixed_existing_nonexisting(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field3": "value3"})

        ctx.cmdtokens = ["hmget", "hash1", "field1", "field2", "field3"]
        cmd = HMGetCommand()
        result = cmd.execute(ctx)

        assert result == ["value1", None, "value3"]

    def test_hmget_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hmget", "nonexistent", "field1", "field2"]
        cmd = HMGetCommand()
        result = cmd.execute(ctx)

        assert result == [None, None]

    def test_hmget_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hmget", "string1", "field1"]
        cmd = HMGetCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHValsCommand:
    def test_hvals_existing_hash(self, ctx):
        ctx.db.set("hash1", {"field1": "value1", "field2": "value2"})

        ctx.cmdtokens = ["hvals", "hash1"]
        cmd = HValsCommand()
        result = cmd.execute(ctx)

        assert set(result) == {"value1", "value2"}

    def test_hvals_empty_hash(self, ctx):
        ctx.db.set("hash1", {})

        ctx.cmdtokens = ["hvals", "hash1"]
        cmd = HValsCommand()
        result = cmd.execute(ctx)

        assert result == []

    def test_hvals_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hvals", "nonexistent"]
        cmd = HValsCommand()
        result = cmd.execute(ctx)

        assert result == []

    def test_hvals_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hvals", "string1"]
        cmd = HValsCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHStrLenCommand:
    def test_hstrlen_existing_field(self, ctx):
        ctx.db.set("hash1", {"field1": "hello", "field2": "world!"})

        ctx.cmdtokens = ["hstrlen", "hash1", "field1"]
        cmd = HStrLenCommand()
        result = cmd.execute(ctx)

        assert result == 5

    def test_hstrlen_nonexistent_field(self, ctx):
        ctx.db.set("hash1", {"field1": "hello"})

        ctx.cmdtokens = ["hstrlen", "hash1", "nonexistent"]
        cmd = HStrLenCommand()
        result = cmd.execute(ctx)

        assert result == 0

    def test_hstrlen_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hstrlen", "nonexistent", "field1"]
        cmd = HStrLenCommand()
        result = cmd.execute(ctx)

        assert result == 0

    def test_hstrlen_numeric_value(self, ctx):
        ctx.db.set("hash1", {"field1": "12345"})

        ctx.cmdtokens = ["hstrlen", "hash1", "field1"]
        cmd = HStrLenCommand()
        result = cmd.execute(ctx)

        assert result == 5

    def test_hstrlen_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hstrlen", "string1", "field1"]
        cmd = HStrLenCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)


class TestHScanCommand:
    def test_hscan_basic_pagination(self, ctx):
        # Create a hash with multiple entries
        ctx.db.set("hash1", {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3",
            "field4": "value4",
            "field5": "value5"
        })

        # First scan with count 2
        ctx.cmdtokens = ["hscan", "hash1", "0", "count", "2"]
        cmd = HScanCommand()
        cursor, items = cmd.execute(ctx)

        assert cursor != 0  # Should not be complete
        assert len(items) == 4  # 2 pairs of field-value
        first_batch = dict(zip(items[::2], items[1::2]))
        assert len(first_batch) == 2

        # Second scan with returned cursor
        ctx.cmdtokens = ["hscan", "hash1", str(cursor), "count", "2"]
        cmd = HScanCommand()
        cursor, items = cmd.execute(ctx)

        second_batch = dict(zip(items[::2], items[1::2]))
        assert len(second_batch) == 2

        # Final scan
        ctx.cmdtokens = ["hscan", "hash1", str(cursor), "count", "2"]
        cmd = HScanCommand()
        cursor, items = cmd.execute(ctx)

        assert cursor == 0  # Indicates completion
        final_batch = dict(zip(items[::2], items[1::2]))
        assert len(final_batch) == 1

        # Verify all items were returned
        all_returned = {**first_batch, **second_batch, **final_batch}
        assert len(all_returned) == 5
        assert all_returned == ctx.db.get("hash1")

    def test_hscan_with_pattern(self, ctx):
        ctx.db.set("hash1", {
            "field1": "value1",
            "field2": "value2",
            "test1": "value3",
            "test2": "value4"
        })

        ctx.cmdtokens = ["hscan", "hash1", "0", "match", "field*", "count", "1"]
        cmd = HScanCommand()
        cursor, items = cmd.execute(ctx)

        first_batch = dict(zip(items[::2], items[1::2]))
        assert all(k.startswith("field") for k in first_batch.keys())

        # Get remaining items
        ctx.cmdtokens = ["hscan", "hash1", str(cursor), "match", "field*", "count", "1"]
        cmd = HScanCommand()
        cursor, items = cmd.execute(ctx)

        second_batch = dict(zip(items[::2], items[1::2]))
        assert all(k.startswith("field") for k in second_batch.keys())

        # Combined results should include only field* entries
        all_returned = {**first_batch, **second_batch}
        assert len(all_returned) == 2
        assert all(k.startswith("field") for k in all_returned.keys())

    def test_hscan_empty_hash(self, ctx):
        ctx.db.set("hash1", {})

        ctx.cmdtokens = ["hscan", "hash1", "0"]
        cmd = HScanCommand()
        cursor, items = cmd.execute(ctx)

        assert cursor == 0
        assert items == []

    def test_hscan_nonexistent_key(self, ctx):
        ctx.cmdtokens = ["hscan", "nonexistent", "0"]
        cmd = HScanCommand()
        cursor, items = cmd.execute(ctx)

        assert cursor == 0
        assert items == []

    def test_hscan_invalid_type(self, ctx):
        ctx.db.set("string1", "not_a_hash")

        ctx.cmdtokens = ["hscan", "string1", "0"]
        cmd = HScanCommand()
        with pytest.raises(TypeError, match="value is not a hash"):
            cmd.execute(ctx)

    def test_hscan_with_large_count(self, ctx):
        # Create a hash with multiple entries
        test_data = {f"field{i}": f"value{i}" for i in range(1, 11)}
        ctx.db.set("hash1", test_data)

        # Request more items than exist
        ctx.cmdtokens = ["hscan", "hash1", "0", "count", "20"]
        cmd = HScanCommand()
        cursor, items = cmd.execute(ctx)

        assert cursor == 0  # Should complete in one iteration
        result_dict = dict(zip(items[::2], items[1::2]))
        assert result_dict == test_data

    def test_hscan_cursor_continuity(self, ctx):
        # Create a hash with multiple entries
        test_data = {f"field{i}": f"value{i}" for i in range(1, 6)}
        ctx.db.set("hash1", test_data)

        # First scan
        ctx.cmdtokens = ["hscan", "hash1", "0", "count", "2"]
        cmd = HScanCommand()
        cursor1, items1 = cmd.execute(ctx)

        # Use invalid cursor
        ctx.cmdtokens = ["hscan", "hash1", "999", "count", "2"]
        cmd = HScanCommand()
        cursor2, items2 = cmd.execute(ctx)

        # Should restart from beginning
        assert len(items2) == len(items1)
        assert dict(zip(items2[::2], items2[1::2])) == dict(zip(items1[::2], items1[1::2]))
