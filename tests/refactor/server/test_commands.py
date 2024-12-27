import time

import pytest

from refactor.server.commands import (
    AppendCommand,
    CopyCommand,
    DecrbyCommand,
    DeleteCommand,
    ExistsCommand,
    ExpireCommand,
    ExpireTimeCommand,
    ExpireatCommand,
    GetCommand,
    IncrbyCommand,
    IncrbyfloatCommand,
    KeysCommand,
    MgetCommand,
    MsetCommand,
    MsetnxCommand,
    PersistCommand,
    RandomKeyCommand,
    RenameCommand,
    RenamenxCommand,
    SetCommand,
    StrlenCommand,
    SubstrCommand,
    TTLCommand,
    TypeCommand,
)

from refactor.server.db import LitedisDb
from refactor.utils import parse_string_command


class TestSetCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_setcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return SetCommand(db=self.db,
                          name=name,
                          args=args)

    def test_execute_with_key_value(self):
        strcmd = "set key1 value1"
        command = self._create_setcommand_from_strcmd(strcmd)
        command.execute()
        assert self.db.get("key1") == "value1"

    @pytest.mark.parametrize("strcmd, expected", [
        ("set key1 value1 nx", None),
        ("set key1 value1 xx", "OK"),
    ])
    def test_execute_with_nx_xx(self, strcmd, expected):
        command = self._create_setcommand_from_strcmd("set key1 value1")
        command.execute()
        command = self._create_setcommand_from_strcmd(strcmd)
        res = command.execute()
        assert res == expected

    @pytest.mark.parametrize("strcmd", [
        "set key1 value1 ex 60",
        "set key1 value1 px 60000",
        f"set key1 value1 exat {int(time.time()) + 60}",
        f"set key1 value1 pxat {int(time.time() * 1000) + 60 * 1000}"
    ])
    def test_execute_with_expiration(self, strcmd):
        command = self._create_setcommand_from_strcmd(strcmd)
        command.execute()
        expiration = self.db.get_expiration("key1")
        assert expiration // 1000 == int(time.time()) + 60


class TestGetCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        # set a key-value for testing
        set_command = SetCommand(db=self.db, name="set", args=["key1", "value1"])
        set_command.execute()

    def _create_getcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return GetCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key(self):
        strcmd = "get key1"
        command = self._create_getcommand_from_strcmd(strcmd)
        result = command.execute()
        assert result == "value1"

    def test_execute_with_non_existing_key(self):
        strcmd = "get key2"
        command = self._create_getcommand_from_strcmd(strcmd)
        result = command.execute()
        assert result is None


class TestAppendCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_appendcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return AppendCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key(self):
        self.db.set("key1", "Hello")

        strcmd = "append key1 World"
        command = self._create_appendcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 10
        assert self.db.get("key1") == "HelloWorld"

    def test_execute_with_non_existing_key(self):
        strcmd = "append key1 World"
        command = self._create_appendcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 5
        assert self.db.get("key1") == "World"


class TestDecrbyCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_decrbycommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return DecrbyCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key(self):
        self.db.set("key1", "10")

        strcmd = "decrby key1 3"
        command = self._create_decrbycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 7
        assert self.db.get("key1") == "7"

    def test_execute_with_non_existing_key(self):
        strcmd = "decrby key1 3"
        command = self._create_decrbycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == -3
        assert self.db.get("key1") == "-3"

    def test_execute_with_non_integer_value(self):
        self.db.set("key1", "abc")

        strcmd = "decrby key1 3"
        command = self._create_decrbycommand_from_strcmd(strcmd)
        with pytest.raises(ValueError, match="value is not an integer"):
            command.execute()


class TestDeleteCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "value1")
        self.db.set("key2", "value2")

    def _create_deletecommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return DeleteCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_keys(self):
        strcmd = "del key1 key2"
        command = self._create_deletecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 2
        assert "key1" not in self.db
        assert "key2" not in self.db

    def test_execute_with_non_existing_keys(self):
        strcmd = "del key3 key4"
        command = self._create_deletecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 0


class TestExistsCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "value1")
        self.db.set("key2", "value2")

    def _create_existscommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return ExistsCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_keys(self):
        strcmd = "exists key1 key2"
        command = self._create_existscommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 2

    def test_execute_with_some_existing_keys(self):
        strcmd = "exists key1 key3"
        command = self._create_existscommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 1

    def test_execute_with_non_existing_keys(self):
        strcmd = "exists key3 key4"
        command = self._create_existscommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 0


class TestCopyCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("source", "value1")

    def _create_copycommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return CopyCommand(db=self.db, name=name, args=args)

    def test_execute_basic_copy(self):
        strcmd = "copy source dest"
        command = self._create_copycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 1
        assert self.db.get("dest") == "value1"

    def test_execute_with_expiration(self):
        expiration = int(time.time() * 1000) + 60000
        self.db.set_expiration("source", expiration)

        strcmd = "copy source dest"
        command = self._create_copycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 1
        assert self.db.get_expiration("dest") == expiration

    def test_execute_with_replace(self):
        self.db.set("dest", "old_value")
        strcmd = "copy source dest replace"
        command = self._create_copycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 1
        assert self.db.get("dest") == "value1"

    def test_execute_without_replace(self):
        self.db.set("dest", "old_value")
        strcmd = "copy source dest"
        command = self._create_copycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 0
        assert self.db.get("dest") == "old_value"


class TestExpireCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "value1")

    def _create_expirecommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return ExpireCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key(self):
        strcmd = "expire key1 60"
        command = self._create_expirecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 1
        expiration = self.db.get_expiration("key1")
        assert expiration // 1000 == int(time.time()) + 60

    def test_execute_with_non_existing_key(self):
        strcmd = "expire key2 60"
        command = self._create_expirecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 0


class TestExpireatCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "value1")

    def _create_expireatcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return ExpireatCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key(self):
        future_time = int(time.time()) + 60
        strcmd = f"expireat key1 {future_time}"
        command = self._create_expireatcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 1
        expiration = self.db.get_expiration("key1")
        assert expiration // 1000 == future_time

    def test_execute_with_non_existing_key(self):
        future_time = int(time.time()) + 60
        strcmd = f"expireat key2 {future_time}"
        command = self._create_expireatcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 0


class TestExpireTimeCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "value1")
        self.future_time = int(time.time()) + 60
        self.db.set_expiration("key1", self.future_time * 1000)

    def _create_expiretimecommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return ExpireTimeCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key_and_expiration(self):
        strcmd = "expiretime key1"
        command = self._create_expiretimecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == self.future_time

    def test_execute_with_existing_key_no_expiration(self):
        self.db.set("key2", "value2")  # Key without expiration
        strcmd = "expiretime key2"
        command = self._create_expiretimecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == -1

    def test_execute_with_non_existing_key(self):
        strcmd = "expiretime key3"
        command = self._create_expiretimecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == -2


class TestIncrbyCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_incrbycommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return IncrbyCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key(self):
        self.db.set("key1", "10")
        strcmd = "incrby key1 5"
        command = self._create_incrbycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 15
        assert self.db.get("key1") == "15"

    def test_execute_with_non_existing_key(self):
        strcmd = "incrby key1 5"
        command = self._create_incrbycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 5
        assert self.db.get("key1") == "5"

    def test_execute_with_non_integer_value(self):
        self.db.set("key1", "abc")
        strcmd = "incrby key1 5"
        command = self._create_incrbycommand_from_strcmd(strcmd)

        with pytest.raises(ValueError, match="value is not an integer"):
            command.execute()


class TestIncrbyfloatCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_incrbyfloatcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return IncrbyfloatCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key(self):
        self.db.set("key1", "10.5")
        strcmd = "incrbyfloat key1 2.5"
        command = self._create_incrbyfloatcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == "13"  # Format removes trailing .0
        assert self.db.get("key1") == "13"

    def test_execute_with_non_existing_key(self):
        strcmd = "incrbyfloat key1 2.5"
        command = self._create_incrbyfloatcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == "2.5"
        assert self.db.get("key1") == "2.5"

    def test_execute_with_non_float_value(self):
        self.db.set("key1", "abc")
        strcmd = "incrbyfloat key1 2.5"
        command = self._create_incrbyfloatcommand_from_strcmd(strcmd)

        with pytest.raises(ValueError, match="value is not a valid float"):
            command.execute()


class TestKeysCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "value1")
        self.db.set("key2", "value2")
        self.db.set("test1", "value3")
        self.db.set("test2", "value4")

    def _create_keyscommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return KeysCommand(db=self.db, name=name, args=args)

    def test_execute_with_exact_match(self):
        strcmd = "keys key1"
        command = self._create_keyscommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == ["key1"]

    def test_execute_with_wildcard(self):
        strcmd = "keys key*"
        command = self._create_keyscommand_from_strcmd(strcmd)
        result = command.execute()

        assert sorted(result) == ["key1", "key2"]

    def test_execute_with_question_mark(self):
        strcmd = "keys test?"
        command = self._create_keyscommand_from_strcmd(strcmd)
        result = command.execute()

        assert sorted(result) == ["test1", "test2"]

    def test_execute_with_no_match(self):
        strcmd = "keys nomatch*"
        command = self._create_keyscommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == []


class TestMgetCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "value1")
        self.db.set("key2", "value2")

    def _create_mgetcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return MgetCommand(db=self.db, name=name, args=args)

    def test_execute_with_all_existing_keys(self):
        strcmd = "mget key1 key2"
        command = self._create_mgetcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == ["value1", "value2"]

    def test_execute_with_some_missing_keys(self):
        strcmd = "mget key1 key3 key2"
        command = self._create_mgetcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == ["value1", None, "value2"]

    def test_execute_with_all_missing_keys(self):
        strcmd = "mget key3 key4"
        command = self._create_mgetcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == [None, None]


class TestMsetCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_msetcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return MsetCommand(db=self.db, name=name, args=args)

    def test_execute_basic_mset(self):
        strcmd = "mset key1 value1 key2 value2"
        command = self._create_msetcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == "OK"
        assert self.db.get("key1") == "value1"
        assert self.db.get("key2") == "value2"

    def test_execute_with_existing_keys(self):
        self.db.set("key1", "old1")
        strcmd = "mset key1 new1 key2 value2"
        command = self._create_msetcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == "OK"
        assert self.db.get("key1") == "new1"


class TestMsetnxCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_msetnxcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return MsetnxCommand(db=self.db, name=name, args=args)

    def test_execute_with_no_existing_keys(self):
        strcmd = "msetnx key1 value1 key2 value2"
        command = self._create_msetnxcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 1
        assert self.db.get("key1") == "value1"
        assert self.db.get("key2") == "value2"

    def test_execute_with_existing_key(self):
        self.db.set("key1", "old1")
        strcmd = "msetnx key1 new1 key2 value2"
        command = self._create_msetnxcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 0
        assert self.db.get("key1") == "old1"
        assert "key2" not in self.db


class TestPersistCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "value1")
        expiration = int(time.time() * 1000) + 60000
        self.db.set_expiration("key1", expiration)

    def _create_persistcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return PersistCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_expiration(self):
        strcmd = "persist key1"
        command = self._create_persistcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 1
        assert self.db.get_expiration("key1") is None

    def test_execute_with_no_expiration(self):
        self.db.set("key2", "value2")
        strcmd = "persist key2"
        command = self._create_persistcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 0


class TestRandomKeyCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_randomkeycommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return RandomKeyCommand(db=self.db, name=name, args=args)

    def test_execute_with_empty_db(self):
        strcmd = "randomkey"
        command = self._create_randomkeycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result is None

    def test_execute_with_keys(self):
        self.db.set("key1", "value1")
        self.db.set("key2", "value2")
        strcmd = "randomkey"
        command = self._create_randomkeycommand_from_strcmd(strcmd)
        result = command.execute()

        assert result in ["key1", "key2"]


class TestRenameCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("source", "value1")

    def _create_renamecommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return RenameCommand(db=self.db, name=name, args=args)

    def test_execute_basic_rename(self):
        strcmd = "rename source dest"
        command = self._create_renamecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == "OK"
        assert "source" not in self.db
        assert self.db.get("dest") == "value1"

    def test_execute_with_expiration(self):
        expiration = int(time.time() * 1000) + 60000
        self.db.set_expiration("source", expiration)
        strcmd = "rename source dest"
        command = self._create_renamecommand_from_strcmd(strcmd)
        command.execute()

        assert self.db.get_expiration("dest") == expiration

    def test_execute_with_non_existing_source(self):
        strcmd = "rename nosource dest"
        command = self._create_renamecommand_from_strcmd(strcmd)
        with pytest.raises(KeyError, match="source key does not exist"):
            command.execute()


class TestRenamenxCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("source", "value1")

    def _create_renamenxcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return RenamenxCommand(db=self.db, name=name, args=args)

    def test_execute_with_non_existing_dest(self):
        strcmd = "renamenx source dest"
        command = self._create_renamenxcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 1
        assert "source" not in self.db
        assert self.db.get("dest") == "value1"

    def test_execute_with_existing_dest(self):
        self.db.set("dest", "old_value")
        strcmd = "renamenx source dest"
        command = self._create_renamenxcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 0
        assert self.db.get("source") == "value1"
        assert self.db.get("dest") == "old_value"


class TestStrlenCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_strlencommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return StrlenCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_key(self):
        self.db.set("key1", "Hello")
        strcmd = "strlen key1"
        command = self._create_strlencommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 5

    def test_execute_with_non_existing_key(self):
        strcmd = "strlen key1"
        command = self._create_strlencommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == 0


class TestSubstrCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "Hello World")

    def _create_substrcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return SubstrCommand(db=self.db, name=name, args=args)

    def test_execute_with_positive_indices(self):
        strcmd = "substr key1 0 4"
        command = self._create_substrcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == "Hello"

    def test_execute_with_negative_indices(self):
        strcmd = "substr key1 -5 -1"
        command = self._create_substrcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == "World"

    def test_execute_with_non_existing_key(self):
        strcmd = "substr key2 0 4"
        command = self._create_substrcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == ""


class TestTTLCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")
        self.db.set("key1", "value1")
        self.expiration = int(time.time() * 1000) + 60000
        self.db.set_expiration("key1", self.expiration)

    def _create_ttlcommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return TTLCommand(db=self.db, name=name, args=args)

    def test_execute_with_existing_expiration(self):
        strcmd = "ttl key1"
        command = self._create_ttlcommand_from_strcmd(strcmd)
        result = command.execute()

        assert 58 <= result <= 60

    def test_execute_with_no_expiration(self):
        self.db.set("key2", "value2")
        strcmd = "ttl key2"
        command = self._create_ttlcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == -1

    def test_execute_with_non_existing_key(self):
        strcmd = "ttl key3"
        command = self._create_ttlcommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == -2


class TestTypeCommand:
    def setup_method(self):
        self.db = LitedisDb("path/to")

    def _create_typecommand_from_strcmd(self, cmd):
        name, args = parse_string_command(cmd)
        return TypeCommand(db=self.db, name=name, args=args)

    def test_execute_with_string(self):
        self.db.set("key1", "Hello")
        strcmd = "type key1"
        command = self._create_typecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == "string"

    def test_execute_with_non_existing_key(self):
        strcmd = "type key1"
        command = self._create_typecommand_from_strcmd(strcmd)
        result = command.execute()

        assert result == "none"
