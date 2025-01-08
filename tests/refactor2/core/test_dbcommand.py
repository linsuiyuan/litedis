import time

import pytest

from refactor2.core.dbcommand import DBCommandConverter
from refactor2.core.persistence import LitedisDB
from refactor2.sortedset import SortedSet
from refactor2.typing import DBCommandTokens


@pytest.fixture
def mock_db():
    db = LitedisDB("test_db")
    yield db


class TestDBCommandTokensConverter:
    def test_dbs_to_commands_basic(self, mock_db):
        mock_db.set("key1", "value1")
        mock_db.set("key2", "value2")

        dbs = {"test_db": mock_db}
        commands = list(DBCommandConverter.dbs_to_commands(dbs))

        assert len(commands) == 2
        assert any(cmd.dbname == "test_db" and cmd.cmdtokens == ['set', 'key1', 'value1'] for cmd in commands)
        assert any(cmd.dbname == "test_db" and cmd.cmdtokens == ['set', 'key2', 'value2'] for cmd in commands)

    def test_dbs_to_commands_with_expiration(self, mock_db):
        mock_db.set("key1", "value1")
        expiration = int(time.time() * 1000) + 10000  # 10 seconds from now
        mock_db.set_expiration("key1", expiration)

        dbs = {"test_db": mock_db}
        commands = list(DBCommandConverter.dbs_to_commands(dbs))

        assert len(commands) == 1
        cmd = commands[0]
        assert cmd.dbname == "test_db"
        assert cmd.cmdtokens == ['set', 'key1', 'value1', 'pxat', f'{expiration}']

    def test_convert_db_object_to_cmdtokens_basic(self, mock_db):
        mock_db.set("key1", "value1")
        cmdtokens = DBCommandConverter._convert_db_object_to_cmdtokens("key1", mock_db)
        assert cmdtokens == ['set', 'key1', 'value1']

    def test_convert_db_object_to_cmdtokens_missing_key(self, mock_db):
        with pytest.raises(KeyError, match="'missing_key' doesn't exist"):
            DBCommandConverter._convert_db_object_to_cmdtokens("missing_key", mock_db)

    def test_convert_db_object_to_cmdtokens_unsupported_type(self, mock_db):
        mock_db._data["invalid_key"] = 123

        with pytest.raises(TypeError, match="the value type the key.*is not supported"):
            DBCommandConverter._convert_db_object_to_cmdtokens("invalid_key", mock_db)

    def test_convert_db_object_to_cmdtokens_hash(self, mock_db):
        mock_db.set("hash_key", {"field1": "val1", "field2": "val2"})
        cmdtokens = DBCommandConverter._convert_db_object_to_cmdtokens("hash_key", mock_db)
        assert cmdtokens == ['hset', 'hash_key', 'field1', 'val1', 'field2', 'val2']

    def test_convert_db_object_to_cmdtokens_list(self, mock_db):
        mock_db.set("list_key", ["item1", "item2", "item3"])
        cmdtokens = DBCommandConverter._convert_db_object_to_cmdtokens("list_key", mock_db)
        assert cmdtokens == ['rpush', 'list_key', 'item1', 'item2', 'item3']

    def test_convert_db_object_to_cmdtokens_set(self, mock_db):
        mock_db.set("set_key", {"member1", "member2", "member3"})
        cmdtokens = DBCommandConverter._convert_db_object_to_cmdtokens("set_key", mock_db)
        # Since sets are unordered, we need to check the components separately
        assert cmdtokens[0] == 'sadd'
        assert cmdtokens[1] == 'set_key'
        assert set(cmdtokens[2:]) == {"member1", "member2", "member3"}

    def test_convert_db_object_to_cmdtokens_sorted_set(self, mock_db):
        from refactor2.sortedset import SortedSet
        zset = SortedSet()
        zset["member1"] = 1.0
        zset["member2"] = 2.0
        mock_db.set("zset_key", zset)
        cmdtokens = DBCommandConverter._convert_db_object_to_cmdtokens("zset_key", mock_db)
        assert cmdtokens == ['zadd', 'zset_key', '1.0', 'member1', '2.0', 'member2']

    def test_dbs_to_commands_all_types(self, mock_db):
        from refactor2.sortedset import SortedSet
        # Set up different types of data
        mock_db.set("str_key", "string_value")
        mock_db.set("hash_key", {"field1": "val1", "field2": "val2"})
        mock_db.set("list_key", ["item1", "item2"])
        mock_db.set("set_key", {"member1", "member2"})

        zset = SortedSet()
        zset["member1"] = 1.0
        zset["member2"] = 2.0
        mock_db.set("zset_key", zset)

        dbs = {"test_db": mock_db}
        commands = list(DBCommandConverter.dbs_to_commands(dbs))

        assert len(commands) == 5
        expected_commands = [
            ('str_key', ['set', 'str_key', 'string_value']),
            ('hash_key', ['hset', 'hash_key', 'field1', 'val1', 'field2', 'val2']),
            ('list_key', ['rpush', 'list_key', 'item1', 'item2']),
            ('zset_key', ['zadd', 'zset_key', '1.0', 'member1', '2.0', 'member2'])
        ]

        # Check each command
        for cmd in commands:
            assert cmd.dbname == "test_db"
            if cmd.cmdtokens[1] == 'set_key':
                # Special handling for set due to unordered nature
                assert cmd.cmdtokens[0] == 'sadd'
                assert cmd.cmdtokens[1] == 'set_key'
                assert set(cmd.cmdtokens[2:]) == {"member1", "member2"}
            else:
                # For other types, we can check the exact command
                assert any(cmd.cmdtokens[1] == key and cmd.cmdtokens == tokens
                           for key, tokens in expected_commands)

    def test_commands_to_dbs_basic(self):
        commands = [
            DBCommandTokens("db1", ['set', 'key1', 'value1']),
            DBCommandTokens("db2", ['set', 'key2', 'value2'])
        ]

        dbs = DBCommandConverter.commands_to_dbs(commands)

        assert len(dbs) == 2
        assert "db1" in dbs
        assert "db2" in dbs
        assert dbs["db1"].get("key1") == "value1"
        assert dbs["db2"].get("key2") == "value2"

    def test_commands_to_dbs_same_db(self):
        commands = [
            DBCommandTokens("db1", ['set', 'key1', 'value1']),
            DBCommandTokens("db1", ['set', 'key2', 'value2'])
        ]

        dbs = DBCommandConverter.commands_to_dbs(commands)

        assert len(dbs) == 1
        assert "db1" in dbs
        assert dbs["db1"].get("key1") == "value1"
        assert dbs["db1"].get("key2") == "value2"

    def test_commands_to_dbs_all_types(self):
        commands = [
            DBCommandTokens("db1", ['set', 'str_key', 'string_value']),
            DBCommandTokens("db1", ['hset', 'hash_key', 'field1', 'val1', 'field2', 'val2']),
            DBCommandTokens("db1", ['rpush', 'list_key', 'item1', 'item2']),
            DBCommandTokens("db1", ['sadd', 'set_key', 'member1', 'member2']),
            DBCommandTokens("db1", ['zadd', 'zset_key', '1.0', 'member1', '2.0', 'member2'])
        ]

        dbs = DBCommandConverter.commands_to_dbs(commands)

        assert len(dbs) == 1
        db = dbs["db1"]

        # Verify string
        assert db.get("str_key") == "string_value"

        # Verify hash
        assert db.get("hash_key") == {"field1": "val1", "field2": "val2"}

        # Verify list
        assert db.get("list_key") == ["item1", "item2"]

        # Verify set
        assert db.get("set_key") == {"member1", "member2"}

        # Verify sorted set
        zset = db.get("zset_key")
        assert isinstance(zset, SortedSet)
        assert dict(zset.items()) == {"member1": 1.0, "member2": 2.0}
