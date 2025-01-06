import time
from unittest.mock import patch

import pytest

from refactor2.core.command.base import CommandContext
from refactor2.core.command.basiccmds import (
    SetCommand,
    GetCommand,
    AppendCommand,
    DecrbyCommand,
    DeleteCommand,
    ExistsCommand,
    CopyCommand,
    ExpireCommand,
    ExpireatCommand,
    ExpireTimeCommand,
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
    StrlenCommand,
    SubstrCommand,
    TTLCommand,
    PTTLCommand,
    TypeCommand,
    UnlinkCommand
)
from refactor2.core.persistence.ldb import LitedisDB
from refactor2.sortedset import SortedSet


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db)


MOCK_TIME_INITIAL_TIMESTAMP = 1000


@pytest.fixture
def mock_time():
    """Fixture to mock time.time() for testing"""
    with patch('time.time') as mock_time:
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP
        yield mock_time


class TestSetCommand:
    def test_basic_set(self, ctx):
        cmd = SetCommand(['set', 'key', 'value'])
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key') == 'value'

    def test_set_with_ex(self, ctx, mock_time):
        cmd = SetCommand(['set', 'key', 'value', 'ex', '1'])
        cmd.execute(ctx)
        assert ctx.db.get('key') == 'value'

        # time pass
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2
        assert ctx.db.get('key') is None

    def test_set_with_px(self, ctx, mock_time):
        cmd = SetCommand(['set', 'key', 'value', 'px', '100'])
        cmd.execute(ctx)
        assert ctx.db.get('key') == 'value'

        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2
        assert ctx.db.get('key') is None

    def test_set_nx(self, ctx):
        # First set should succeed
        cmd = SetCommand(['set', 'key', 'value1', 'nx'])
        result = cmd.execute(ctx)
        assert result == 'OK'

        # Second set should fail
        cmd = SetCommand(['set', 'key', 'value2', 'nx'])
        result = cmd.execute(ctx)
        assert result is None
        assert ctx.db.get('key') == 'value1'

    def test_set_xx(self, ctx):
        # Should fail when key doesn't exist
        cmd = SetCommand(['set', 'key', 'value1', 'xx'])
        result = cmd.execute(ctx)
        assert result is None

        # Set key first
        ctx.db.set('key', 'oldvalue')

        # Should succeed when key exists
        cmd = SetCommand(['set', 'key', 'value2', 'xx'])
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key') == 'value2'

    def test_set_get(self, ctx):
        # Set initial value
        ctx.db.set('key', 'oldvalue')

        # Set with GET option
        cmd = SetCommand(['set', 'key', 'newvalue', 'get'])
        result = cmd.execute(ctx)
        assert result == 'oldvalue'
        assert ctx.db.get('key') == 'newvalue'

    def test_set_keepttl(self, ctx):
        # Set with expiration
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))  # 5 seconds

        # Set with KEEPTTL
        cmd = SetCommand(['set', 'key', 'newvalue', 'keepttl'])
        cmd.execute(ctx)

        assert ctx.db.exists_expiration('key')

    def test_invalid_options(self, ctx):
        with pytest.raises(ValueError):
            SetCommand(['set', 'key', 'value', 'nx', 'xx'])

        with pytest.raises(ValueError):
            SetCommand(['set', 'key', 'value', 'ex', '-1'])


class TestGetCommand:
    def test_get_existing_key(self, ctx):
        ctx.db.set('key', 'value')
        cmd = GetCommand(['get', 'key'])
        result = cmd.execute(ctx)
        assert result == 'value'

    def test_get_nonexistent_key(self, ctx):
        cmd = GetCommand(['get', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result is None

    def test_get_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])  # Set non-string value
        cmd = GetCommand(['get', 'key'])
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestAppendCommand:
    def test_append_existing_string(self, ctx):
        ctx.db.set('key', 'Hello')
        cmd = AppendCommand(['append', 'key', ' World'])
        result = cmd.execute(ctx)
        assert result == 11
        assert ctx.db.get('key') == 'Hello World'

    def test_append_nonexistent_key(self, ctx):
        cmd = AppendCommand(['append', 'key', 'World'])
        result = cmd.execute(ctx)
        assert result == 5
        assert ctx.db.get('key') == 'World'

    def test_append_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        cmd = AppendCommand(['append', 'key', 'value'])
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestDecrbyCommand:
    def test_decrby_existing_number(self, ctx):
        ctx.db.set('key', '10')
        cmd = DecrbyCommand(['decrby', 'key', '3'])
        result = cmd.execute(ctx)
        assert result == '7'

    def test_decrby_nonexistent_key(self, ctx):
        cmd = DecrbyCommand(['decrby', 'key', '5'])
        result = cmd.execute(ctx)
        assert result == '-5'

    def test_decrby_non_integer(self, ctx):
        ctx.db.set('key', 'abc')
        cmd = DecrbyCommand(['decrby', 'key', '5'])
        with pytest.raises(ValueError):
            cmd.execute(ctx)

    def test_decrby_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        cmd = DecrbyCommand(['decrby', 'key', '5'])
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestDeleteCommand:
    def test_delete_single_existing_key(self, ctx):
        ctx.db.set('key', 'value')
        cmd = DeleteCommand(['del', 'key'])
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists('key')

    def test_delete_multiple_keys(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('key3', 'value3')
        cmd = DeleteCommand(['del', 'key1', 'key2', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == 2
        assert not ctx.db.exists('key1')
        assert not ctx.db.exists('key2')
        assert ctx.db.exists('key3')

    def test_delete_nonexistent_key(self, ctx):
        cmd = DeleteCommand(['del', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == 0


class TestExistsCommand:
    def test_exists_single_key(self, ctx):
        ctx.db.set('key', 'value')
        cmd = ExistsCommand(['exists', 'key'])
        result = cmd.execute(ctx)
        assert result == 1

    def test_exists_multiple_keys(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        cmd = ExistsCommand(['exists', 'key1', 'key2', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == 2

    def test_exists_nonexistent_keys(self, ctx):
        cmd = ExistsCommand(['exists', 'nonexistent1', 'nonexistent2'])
        result = cmd.execute(ctx)
        assert result == 0


class TestCopyCommand:
    def test_copy_basic(self, ctx):
        ctx.db.set('source', 'value')
        cmd = CopyCommand(['copy', 'source', 'dest'])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('dest') == 'value'
        assert ctx.db.get('source') == 'value'

    def test_copy_with_expiration(self, ctx):
        # Set source with expiration
        ctx.db.set('source', 'value')
        expiration = int(time.time() * 1000 + 5000)  # 5 seconds from now
        ctx.db.set_expiration('source', expiration)

        cmd = CopyCommand(['copy', 'source', 'dest'])
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get('dest') == 'value'
        assert ctx.db.exists_expiration('dest')
        assert ctx.db.get_expiration('dest') == expiration

    def test_copy_nonexistent_source(self, ctx):
        cmd = CopyCommand(['copy', 'nonexistent', 'dest'])
        result = cmd.execute(ctx)
        assert result == 0
        assert not ctx.db.exists('dest')

    def test_copy_existing_destination_no_replace(self, ctx):
        ctx.db.set('source', 'value1')
        ctx.db.set('dest', 'value2')
        cmd = CopyCommand(['copy', 'source', 'dest'])
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.get('dest') == 'value2'

    def test_copy_existing_destination_with_replace(self, ctx):
        ctx.db.set('source', 'value1')
        ctx.db.set('dest', 'value2')
        cmd = CopyCommand(['copy', 'source', 'dest', 'replace'])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('dest') == 'value1'


class TestExpireCommand:
    def test_expire_basic(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        cmd = ExpireCommand(['expire', 'key', '1'])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.exists_expiration('key')

        # Simulate the passage of time
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 6
        assert ctx.db.get('key') is None

    def test_expire_nonexistent_key(self, ctx):
        cmd = ExpireCommand(['expire', 'nonexistent', '1'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_expire_negative_seconds(self, ctx):
        ctx.db.set('key', 'value')
        with pytest.raises(ValueError):
            ExpireCommand(['expire', 'key', '-1'])

    def test_expire_update_existing(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        # set initial expiration to 5s
        cmd1 = ExpireCommand(['expire', 'key', '5'])
        cmd1.execute(ctx)

        # update to 10s
        cmd2 = ExpireCommand(['expire', 'key', '10'])
        result = cmd2.execute(ctx)
        assert result == 1

        # check that key exists after 8s
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 8
        assert ctx.db.get('key') == 'value'

        # check that key not exists after 12s
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 12
        assert ctx.db.get('key') is None


class TestExpireatCommand:
    def test_expireat_basic(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        future_timestamp = int(time.time()) + 1
        cmd = ExpireatCommand(['expireat', 'key', str(future_timestamp)])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.exists_expiration('key')

        # time pass
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2
        assert ctx.db.get('key') is None

    def test_expireat_past_timestamp(self, ctx):
        ctx.db.set('key', 'value')
        past_timestamp = int(time.time()) - 1
        cmd = ExpireatCommand(['expireat', 'key', str(past_timestamp)])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('key') is None

    def test_expireat_nonexistent_key(self, ctx):
        cmd = ExpireatCommand(['expireat', 'nonexistent', str(int(time.time()))])
        result = cmd.execute(ctx)
        assert result == 0

    def test_expireat_invalid_timestamp(self, ctx):
        ctx.db.set('key', 'value')
        with pytest.raises(ValueError):
            ExpireatCommand(['expireat', 'key', 'invalid'])


class TestExpireTimeCommand:
    def test_expiretime_with_expiration(self, ctx):
        ctx.db.set('key', 'value')
        future_timestamp = int(time.time()) + 10
        ctx.db.set_expiration('key', future_timestamp * 1000)  # Convert to milliseconds

        cmd = ExpireTimeCommand(['expiretime', 'key'])
        result = cmd.execute(ctx)
        assert result == future_timestamp

    def test_expiretime_no_expiration(self, ctx):
        ctx.db.set('key', 'value')
        cmd = ExpireTimeCommand(['expiretime', 'key'])
        result = cmd.execute(ctx)
        assert result == -1

    def test_expiretime_nonexistent_key(self, ctx):
        cmd = ExpireTimeCommand(['expiretime', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == -2

    def test_expiretime_after_expire(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        # Set expiration to 1 second from now
        future_timestamp = int(time.time()) + 1
        ctx.db.set_expiration('key', future_timestamp * 1000)

        # time pass
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2
        cmd = ExpireTimeCommand(['expiretime', 'key'])
        result = cmd.execute(ctx)
        assert result == -2  # Key should be expired and deleted


class TestIncrbyCommand:
    def test_incrby_existing_number(self, ctx):
        ctx.db.set('key', '10')
        cmd = IncrbyCommand(['incrby', 'key', '5'])
        result = cmd.execute(ctx)
        assert result == '15'
        assert ctx.db.get('key') == '15'

    def test_incrby_nonexistent_key(self, ctx):
        cmd = IncrbyCommand(['incrby', 'key', '5'])
        result = cmd.execute(ctx)
        assert result == '5'
        assert ctx.db.get('key') == '5'

    def test_incrby_negative_number(self, ctx):
        ctx.db.set('key', '10')
        cmd = IncrbyCommand(['incrby', 'key', '-5'])
        result = cmd.execute(ctx)
        assert result == '5'

    def test_incrby_non_integer_string(self, ctx):
        ctx.db.set('key', 'abc')
        cmd = IncrbyCommand(['incrby', 'key', '5'])
        with pytest.raises(ValueError):
            cmd.execute(ctx)

    def test_incrby_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        cmd = IncrbyCommand(['incrby', 'key', '5'])
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestIncrbyfloatCommand:
    def test_incrbyfloat_existing_number(self, ctx):
        ctx.db.set('key', '10.5')
        cmd = IncrbyfloatCommand(['incrbyfloat', 'key', '2.5'])
        result = cmd.execute(ctx)
        assert result == '13'
        assert ctx.db.get('key') == '13'

    def test_incrbyfloat_nonexistent_key(self, ctx):
        cmd = IncrbyfloatCommand(['incrbyfloat', 'key', '5.5'])
        result = cmd.execute(ctx)
        assert result == '5.5'
        assert ctx.db.get('key') == '5.5'

    def test_incrbyfloat_negative_number(self, ctx):
        ctx.db.set('key', '10.5')
        cmd = IncrbyfloatCommand(['incrbyfloat', 'key', '-2.5'])
        result = cmd.execute(ctx)
        assert result == '8'

    def test_incrbyfloat_integer_value(self, ctx):
        ctx.db.set('key', '10')
        cmd = IncrbyfloatCommand(['incrbyfloat', 'key', '5.5'])
        result = cmd.execute(ctx)
        assert result == '15.5'

    def test_incrbyfloat_non_numeric_string(self, ctx):
        ctx.db.set('key', 'abc')
        cmd = IncrbyfloatCommand(['incrbyfloat', 'key', '5.5'])
        with pytest.raises(ValueError):
            cmd.execute(ctx)

    def test_incrbyfloat_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        cmd = IncrbyfloatCommand(['incrbyfloat', 'key', '5.5'])
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestKeysCommand:
    def test_keys_exact_match(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        cmd = KeysCommand(['keys', 'key1'])
        result = cmd.execute(ctx)
        assert result == ['key1']

    def test_keys_wildcard_all(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('other', 'value3')
        cmd = KeysCommand(['keys', '*'])
        result = cmd.execute(ctx)
        assert sorted(result) == ['key1', 'key2', 'other']

    def test_keys_question_mark(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('key3', 'value3')
        cmd = KeysCommand(['keys', 'key?'])
        result = cmd.execute(ctx)
        assert sorted(result) == ['key1', 'key2', 'key3']

    def test_keys_brackets(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('key3', 'value3')
        cmd = KeysCommand(['keys', 'key[1-2]'])
        result = cmd.execute(ctx)
        assert sorted(result) == ['key1', 'key2']

    def test_keys_escaped_special_chars(self, ctx):
        ctx.db.set('key*1', 'value1')
        ctx.db.set('key?2', 'value2')
        cmd1 = KeysCommand(['keys', 'key\\*1'])
        result1 = cmd1.execute(ctx)
        assert result1 == ['key*1']

        cmd2 = KeysCommand(['keys', 'key\\?2'])
        result2 = cmd2.execute(ctx)
        assert result2 == ['key?2']

    def test_keys_no_match(self, ctx):
        ctx.db.set('key1', 'value1')
        cmd = KeysCommand(['keys', 'nonexistent*'])
        result = cmd.execute(ctx)
        assert result == []


class TestMgetCommand:
    def test_mget_all_existing(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        cmd = MgetCommand(['mget', 'key1', 'key2'])
        result = cmd.execute(ctx)
        assert result == ['value1', 'value2']

    def test_mget_some_missing(self, ctx):
        ctx.db.set('key1', 'value1')
        cmd = MgetCommand(['mget', 'key1', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == ['value1', None]

    def test_mget_all_missing(self, ctx):
        cmd = MgetCommand(['mget', 'nonexistent1', 'nonexistent2'])
        result = cmd.execute(ctx)
        assert result == [None, None]

    def test_mget_wrong_type(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', [1, 2, 3])  # Wrong type
        cmd = MgetCommand(['mget', 'key1', 'key2'])
        result = cmd.execute(ctx)
        assert result[0] == 'value1'
        assert result[1] is not None  # The wrong type value should still be returned


class TestMsetCommand:
    def test_mset_basic(self, ctx):
        cmd = MsetCommand(['mset', 'key1', 'value1', 'key2', 'value2'])
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key1') == 'value1'
        assert ctx.db.get('key2') == 'value2'

    def test_mset_override_existing(self, ctx):
        ctx.db.set('key1', 'oldvalue1')
        cmd = MsetCommand(['mset', 'key1', 'newvalue1', 'key2', 'value2'])
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key1') == 'newvalue1'
        assert ctx.db.get('key2') == 'value2'

    def test_mset_invalid_pairs(self, ctx):
        with pytest.raises(ValueError):
            MsetCommand(['mset', 'key1', 'value1', 'key2'])  # Missing value for key2

    def test_mset_empty_values(self, ctx):
        cmd = MsetCommand(['mset', 'key1', '', 'key2', ''])
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key1') == ''
        assert ctx.db.get('key2') == ''

    def test_mset_with_spaces(self, ctx):
        cmd = MsetCommand(['mset', 'key 1', 'value 1', 'key 2', 'value 2'])
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key 1') == 'value 1'
        assert ctx.db.get('key 2') == 'value 2'


class TestMsetnxCommand:
    def test_msetnx_all_new_keys(self, ctx):
        cmd = MsetnxCommand(['msetnx', 'key1', 'value1', 'key2', 'value2'])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('key1') == 'value1'
        assert ctx.db.get('key2') == 'value2'

    def test_msetnx_with_existing_key(self, ctx):
        ctx.db.set('key1', 'oldvalue')
        cmd = MsetnxCommand(['msetnx', 'key1', 'value1', 'key2', 'value2'])
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.get('key1') == 'oldvalue'
        assert not ctx.db.exists('key2')

    def test_msetnx_invalid_pairs(self, ctx):
        with pytest.raises(ValueError):
            MsetnxCommand(['msetnx', 'key1'])  # Missing value

    def test_msetnx_empty_values(self, ctx):
        cmd = MsetnxCommand(['msetnx', 'key1', '', 'key2', ''])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('key1') == ''
        assert ctx.db.get('key2') == ''


class TestPersistCommand:
    def test_persist_with_expiration(self, ctx):
        # Set key with expiration
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))  # 5 seconds

        cmd = PersistCommand(['persist', 'key'])
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists_expiration('key')
        assert ctx.db.get('key') == 'value'

    def test_persist_without_expiration(self, ctx):
        ctx.db.set('key', 'value')
        cmd = PersistCommand(['persist', 'key'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_persist_nonexistent_key(self, ctx):
        cmd = PersistCommand(['persist', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_persist_after_expiration(self, ctx, mock_time):
        # Set key with 1 second expiration
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 1000))

        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2

        cmd = PersistCommand(['persist', 'key'])
        result = cmd.execute(ctx)
        assert result == 0
        assert not ctx.db.exists('key')


class TestRandomKeyCommand:
    def test_random_key_with_keys(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('key3', 'value3')

        cmd = RandomKeyCommand(['randomkey'])
        result = cmd.execute(ctx)
        assert result in ['key1', 'key2', 'key3']

    def test_random_key_empty_db(self, ctx):
        cmd = RandomKeyCommand(['randomkey'])
        result = cmd.execute(ctx)
        assert result is None

    def test_random_key_extra_args(self, ctx):
        with pytest.raises(ValueError):
            RandomKeyCommand(['randomkey', 'extra'])


class TestRenameCommand:
    def test_rename_basic(self, ctx):
        ctx.db.set('old', 'value')
        cmd = RenameCommand(['rename', 'old', 'new'])
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert not ctx.db.exists('old')
        assert ctx.db.get('new') == 'value'

    def test_rename_with_expiration(self, ctx):
        # Set key with expiration
        ctx.db.set('old', 'value')
        expiration = int(time.time() * 1000 + 5000)  # 5 seconds
        ctx.db.set_expiration('old', expiration)

        cmd = RenameCommand(['rename', 'old', 'new'])
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.exists_expiration('new')
        assert ctx.db.get_expiration('new') == expiration

    def test_rename_overwrite_destination(self, ctx):
        ctx.db.set('old', 'value1')
        ctx.db.set('new', 'value2')
        cmd = RenameCommand(['rename', 'old', 'new'])
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert not ctx.db.exists('old')
        assert ctx.db.get('new') == 'value1'

    def test_rename_nonexistent_key(self, ctx):
        with pytest.raises(ValueError):
            RenameCommand(['rename', 'nonexistent', 'new']).execute(ctx)

    def test_rename_same_key(self, ctx):
        ctx.db.set('key', 'value')
        with pytest.raises(ValueError):
            RenameCommand(['rename', 'key', 'key']).execute(ctx)


class TestRenamenxCommand:
    def test_renamenx_basic(self, ctx):
        ctx.db.set('old', 'value')
        cmd = RenamenxCommand(['renamenx', 'old', 'new'])
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists('old')
        assert ctx.db.get('new') == 'value'

    def test_renamenx_existing_destination(self, ctx):
        ctx.db.set('old', 'value1')
        ctx.db.set('new', 'value2')
        cmd = RenamenxCommand(['renamenx', 'old', 'new'])
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.exists('old')
        assert ctx.db.get('new') == 'value2'

    def test_renamenx_with_expiration(self, ctx):
        # Set key with expiration
        ctx.db.set('old', 'value')
        expiration = int(time.time() * 1000 + 5000)  # 5 seconds
        ctx.db.set_expiration('old', expiration)

        cmd = RenamenxCommand(['renamenx', 'old', 'new'])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.exists_expiration('new')
        assert ctx.db.get_expiration('new') == expiration

    def test_renamenx_nonexistent_key(self, ctx):
        with pytest.raises(ValueError):
            RenamenxCommand(['renamenx', 'nonexistent', 'new']).execute(ctx)

    def test_renamenx_same_key(self, ctx):
        with pytest.raises(ValueError):
            RenamenxCommand(['renamenx', 'key1', 'key1']).execute(ctx)


class TestStrlenCommand:
    def test_strlen_basic(self, ctx):
        ctx.db.set('key', 'hello')
        cmd = StrlenCommand(['strlen', 'key'])
        result = cmd.execute(ctx)
        assert result == 5

    def test_strlen_empty_string(self, ctx):
        ctx.db.set('key', '')
        cmd = StrlenCommand(['strlen', 'key'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_strlen_nonexistent_key(self, ctx):
        cmd = StrlenCommand(['strlen', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_strlen_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        cmd = StrlenCommand(['strlen', 'key'])
        with pytest.raises(TypeError):
            cmd.execute(ctx)

    def test_strlen_unicode(self, ctx):
        ctx.db.set('key', '你好世界')  # Chinese characters
        cmd = StrlenCommand(['strlen', 'key'])
        result = cmd.execute(ctx)
        assert result == 4


class TestSubstrCommand:
    def test_substr_basic(self, ctx):
        ctx.db.set('key', 'Hello World')
        cmd = SubstrCommand(['substr', 'key', '0', '4'])
        result = cmd.execute(ctx)
        assert result == 'Hello'

    def test_substr_negative_indices(self, ctx):
        ctx.db.set('key', 'Hello World')
        cmd = SubstrCommand(['substr', 'key', '-5', '-1'])
        result = cmd.execute(ctx)
        assert result == 'World'

    def test_substr_out_of_range(self, ctx):
        ctx.db.set('key', 'Hello')
        cmd = SubstrCommand(['substr', 'key', '0', '10'])
        result = cmd.execute(ctx)
        assert result == 'Hello'

    def test_substr_nonexistent_key(self, ctx):
        cmd = SubstrCommand(['substr', 'nonexistent', '0', '1'])
        result = cmd.execute(ctx)
        assert result is None

    def test_substr_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        cmd = SubstrCommand(['substr', 'key', '0', '1'])
        with pytest.raises(TypeError):
            cmd.execute(ctx)

    def test_substr_invalid_indices(self, ctx):
        ctx.db.set('key', 'Hello')
        with pytest.raises(ValueError):
            SubstrCommand(['substr', 'key', 'invalid', '1'])


class TestTTLCommand:
    def test_ttl_with_expiration(self, ctx):
        ctx.db.set('key', 'value')
        # Set expiration to 5 seconds from now
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))

        cmd = TTLCommand(['ttl', 'key'])
        result = cmd.execute(ctx)
        assert 0 <= result <= 5

    def test_ttl_no_expiration(self, ctx):
        ctx.db.set('key', 'value')
        cmd = TTLCommand(['ttl', 'key'])
        result = cmd.execute(ctx)
        assert result == -1

    def test_ttl_nonexistent_key(self, ctx):
        cmd = TTLCommand(['ttl', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == -2

    def test_ttl_after_expire(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 1000))  # 1 second

        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2

        cmd = TTLCommand(['ttl', 'key'])
        result = cmd.execute(ctx)
        assert result == -2


class TestPTTLCommand:
    def test_pttl_with_expiration(self, ctx):
        ctx.db.set('key', 'value')
        # Set expiration to 5 seconds from now
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))

        cmd = PTTLCommand(['pttl', 'key'])
        result = cmd.execute(ctx)
        assert 0 <= result <= 5000

    def test_pttl_no_expiration(self, ctx):
        ctx.db.set('key', 'value')
        cmd = PTTLCommand(['pttl', 'key'])
        result = cmd.execute(ctx)
        assert result == -1

    def test_pttl_nonexistent_key(self, ctx):
        cmd = PTTLCommand(['pttl', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == -2

    def test_pttl_after_expire(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 1000))  # 1 second

        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2

        cmd = PTTLCommand(['pttl', 'key'])
        result = cmd.execute(ctx)
        assert result == -2


class TestTypeCommand:
    def test_type_string(self, ctx):
        ctx.db.set('key', 'value')
        cmd = TypeCommand(['type', 'key'])
        result = cmd.execute(ctx)
        assert result == 'string'

    def test_type_list(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        cmd = TypeCommand(['type', 'key'])
        result = cmd.execute(ctx)
        assert result == 'list'

    def test_type_hash(self, ctx):
        ctx.db.set('key', {'field': 'value'})
        cmd = TypeCommand(['type', 'key'])
        result = cmd.execute(ctx)
        assert result == 'hash'

    def test_type_set(self, ctx):
        ctx.db.set('key', {1, 2, 3})
        cmd = TypeCommand(['type', 'key'])
        result = cmd.execute(ctx)
        assert result == 'set'

    def test_type_zset(self, ctx):
        ctx.db.set('key', SortedSet({'member': 1.}))
        cmd = TypeCommand(['type', 'key'])
        result = cmd.execute(ctx)
        assert result == 'zset'

    def test_type_nonexistent_key(self, ctx):
        cmd = TypeCommand(['type', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == 'none'


class TestUnlinkCommand:
    def test_unlink_single_key(self, ctx):
        ctx.db.set('key', 'value')
        cmd = UnlinkCommand(['unlink', 'key'])
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists('key')

    def test_unlink_multiple_keys(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('key3', 'value3')
        cmd = UnlinkCommand(['unlink', 'key1', 'key2', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == 2
        assert not ctx.db.exists('key1')
        assert not ctx.db.exists('key2')
        assert ctx.db.exists('key3')

    def test_unlink_nonexistent_keys(self, ctx):
        cmd = UnlinkCommand(['unlink', 'nonexistent1', 'nonexistent2'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_unlink_with_expiration(self, ctx):
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))
        cmd = UnlinkCommand(['unlink', 'key'])
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists('key')
        assert not ctx.db.exists_expiration('key')
