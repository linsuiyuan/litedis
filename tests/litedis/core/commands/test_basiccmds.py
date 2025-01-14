import time
from unittest.mock import patch

import pytest

from litedis.core.command.base import CommandContext
from litedis.core.command.basiccmds import (
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
    MGetCommand,
    MSetCommand,
    MSetnxCommand,
    PersistCommand,
    RandomKeyCommand,
    RenameCommand,
    RenamenxCommand,
    StrlenCommand,
    SubstrCommand,
    TTLCommand,
    PTTLCommand,
    TypeCommand,
)
from litedis.core.command.sortedset import SortedSet
from litedis.core.persistence.ldb import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db, [])


MOCK_TIME_INITIAL_TIMESTAMP = 1000


@pytest.fixture
def mock_time():
    """Fixture to mock time.time() for testing"""
    with patch('time.time') as mock_time:
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP
        yield mock_time


class TestSetCommand:
    def test_basic_set(self, ctx):
        cmd = SetCommand()
        ctx.cmdtokens = ['set', 'key', 'value']
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key') == 'value'

    def test_set_with_ex(self, ctx, mock_time):
        ctx.cmdtokens = ['set', 'key', 'value', 'ex', '1']
        cmd = SetCommand()
        cmd.execute(ctx)
        assert ctx.db.get('key') == 'value'

        # time pass
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2
        assert ctx.db.get('key') is None

    def test_set_with_px(self, ctx, mock_time):
        ctx.cmdtokens = ['set', 'key', 'value', 'px', '100']
        cmd = SetCommand()
        cmd.execute(ctx)
        assert ctx.db.get('key') == 'value'

        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2
        assert ctx.db.get('key') is None

    def test_set_nx(self, ctx):
        # First set should succeed
        ctx.cmdtokens = ['set', 'key', 'value1', 'nx']
        cmd = SetCommand()
        result = cmd.execute(ctx)
        assert result == 'OK'

        # Second set should fail
        ctx.cmdtokens = ['set', 'key', 'value2', 'nx']
        cmd = SetCommand()
        result = cmd.execute(ctx)
        assert result is None
        assert ctx.db.get('key') == 'value1'

    def test_set_xx(self, ctx):
        # Should fail when key doesn't exist
        ctx.cmdtokens = ['set', 'key', 'value1', 'xx']
        cmd = SetCommand()
        result = cmd.execute(ctx)
        assert result is None

        # Set key first
        ctx.db.set('key', 'oldvalue')

        # Should succeed when key exists
        ctx.cmdtokens = ['set', 'key', 'value2', 'xx']
        cmd = SetCommand()
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key') == 'value2'

    def test_set_get(self, ctx):
        # Set initial value
        ctx.db.set('key', 'oldvalue')

        # Set with GET option
        ctx.cmdtokens = ['set', 'key', 'newvalue', 'get']
        cmd = SetCommand()
        result = cmd.execute(ctx)
        assert result == 'oldvalue'
        assert ctx.db.get('key') == 'newvalue'

    def test_set_keepttl(self, ctx):
        # Set with expiration
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))  # 5 seconds

        # Set with KEEPTTL
        ctx.cmdtokens = ['set', 'key', 'newvalue', 'keepttl']
        cmd = SetCommand()
        cmd.execute(ctx)

        assert ctx.db.exists_expiration('key')

    def test_invalid_options(self, ctx):
        ctx.cmdtokens = ['set', 'key', 'value', 'nx', 'xx']
        with pytest.raises(ValueError):
            SetCommand().execute(ctx)

        ctx.cmdtokens = ['set', 'key', 'value', 'ex', '-1']
        with pytest.raises(ValueError):
            SetCommand().execute(ctx)


class TestGetCommand:
    def test_get_existing_key(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['get', 'key']
        cmd = GetCommand()
        result = cmd.execute(ctx)
        assert result == 'value'

    def test_get_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['get', 'nonexistent']
        cmd = GetCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_get_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])  # Set non-string value
        ctx.cmdtokens = ['get', 'key']
        cmd = GetCommand()
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestAppendCommand:
    def test_append_existing_string(self, ctx):
        ctx.db.set('key', 'Hello')
        ctx.cmdtokens = ['append', 'key', ' World']
        cmd = AppendCommand()
        result = cmd.execute(ctx)
        assert result == 11
        assert ctx.db.get('key') == 'Hello World'

    def test_append_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['append', 'key', 'World']
        cmd = AppendCommand()
        result = cmd.execute(ctx)
        assert result == 5
        assert ctx.db.get('key') == 'World'

    def test_append_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        ctx.cmdtokens = ['append', 'key', 'value']
        cmd = AppendCommand()
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestDecrbyCommand:
    def test_decrby_existing_number(self, ctx):
        ctx.db.set('key', '10')
        ctx.cmdtokens = ['decrby', 'key', '3']
        cmd = DecrbyCommand()
        result = cmd.execute(ctx)
        assert result == '7'

    def test_decrby_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['decrby', 'key', '5']
        cmd = DecrbyCommand()
        result = cmd.execute(ctx)
        assert result == '-5'

    def test_decrby_non_integer(self, ctx):
        ctx.db.set('key', 'abc')
        ctx.cmdtokens = ['decrby', 'key', '5']
        cmd = DecrbyCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)

    def test_decrby_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        ctx.cmdtokens = ['decrby', 'key', '5']
        cmd = DecrbyCommand()
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestDeleteCommand:
    def test_delete_single_existing_key(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['del', 'key']
        cmd = DeleteCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists('key')

    def test_delete_multiple_keys(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('key3', 'value3')
        ctx.cmdtokens = ['del', 'key1', 'key2', 'nonexistent']
        cmd = DeleteCommand()
        result = cmd.execute(ctx)
        assert result == 2
        assert not ctx.db.exists('key1')
        assert not ctx.db.exists('key2')
        assert ctx.db.exists('key3')

    def test_delete_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['del', 'nonexistent']
        cmd = DeleteCommand()
        result = cmd.execute(ctx)
        assert result == 0


class TestExistsCommand:
    def test_exists_single_key(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['exists', 'key']
        cmd = ExistsCommand()
        result = cmd.execute(ctx)
        assert result == 1

    def test_exists_multiple_keys(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.cmdtokens = ['exists', 'key1', 'key2', 'nonexistent']
        cmd = ExistsCommand()
        result = cmd.execute(ctx)
        assert result == 2

    def test_exists_nonexistent_keys(self, ctx):
        ctx.cmdtokens = ['exists', 'nonexistent1', 'nonexistent2']
        cmd = ExistsCommand()
        result = cmd.execute(ctx)
        assert result == 0


class TestCopyCommand:
    def test_copy_basic(self, ctx):
        ctx.db.set('source', 'value')
        ctx.cmdtokens = ['copy', 'source', 'dest']
        cmd = CopyCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('dest') == 'value'
        assert ctx.db.get('source') == 'value'

    def test_copy_with_expiration(self, ctx):
        # Set source with expiration
        ctx.db.set('source', 'value')
        expiration = int(time.time() * 1000 + 5000)  # 5 seconds from now
        ctx.db.set_expiration('source', expiration)

        ctx.cmdtokens = ['copy', 'source', 'dest']
        cmd = CopyCommand()
        result = cmd.execute(ctx)

        assert result == 1
        assert ctx.db.get('dest') == 'value'
        assert ctx.db.exists_expiration('dest')
        assert ctx.db.get_expiration('dest') == expiration

    def test_copy_nonexistent_source(self, ctx):
        ctx.cmdtokens = ['copy', 'nonexistent', 'dest']
        cmd = CopyCommand()
        result = cmd.execute(ctx)
        assert result == 0
        assert not ctx.db.exists('dest')

    def test_copy_existing_destination_no_replace(self, ctx):
        ctx.db.set('source', 'value1')
        ctx.db.set('dest', 'value2')
        ctx.cmdtokens = ['copy', 'source', 'dest']
        cmd = CopyCommand()
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.get('dest') == 'value2'

    def test_copy_existing_destination_with_replace(self, ctx):
        ctx.db.set('source', 'value1')
        ctx.db.set('dest', 'value2')
        ctx.cmdtokens = ['copy', 'source', 'dest', 'replace']
        cmd = CopyCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('dest') == 'value1'


class TestExpireCommand:
    def test_expire_basic(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['expire', 'key', '1']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.exists_expiration('key')

        # Simulate the passage of time
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2
        assert ctx.db.get('key') is None

    def test_expire_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['expire', 'key', '1']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_expire_negative_seconds(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['expire', 'key', '-1']
        cmd = ExpireCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)

    def test_expire_nx_option(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['expire', 'key', '10', 'NX']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 1

        # Test NX when key has expiration
        ctx.cmdtokens = ['expire', 'key', '20', 'NX']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_expire_xx_option(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['expire', 'key', '10', 'XX']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 0

        # Set expiration and try again
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))
        ctx.cmdtokens = ['expire', 'key', '20', 'XX']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 1

    def test_expire_gt_option(self, ctx):
        ctx.db.set('key', 'value')
        initial_expiry = int(time.time() * 1000 + 10000)  # 10 seconds
        ctx.db.set_expiration('key', initial_expiry)

        # Try setting smaller expiration with GT
        ctx.cmdtokens = ['expire', 'key', '5', 'GT']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.get_expiration('key') == initial_expiry

        # Try setting larger expiration with GT
        ctx.cmdtokens = ['expire', 'key', '15', 'GT']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 1

    def test_expire_lt_option(self, ctx):
        ctx.db.set('key', 'value')
        initial_expiry = int(time.time() * 1000 + 10000)  # 10 seconds
        ctx.db.set_expiration('key', initial_expiry)

        # Try setting larger expiration with LT
        ctx.cmdtokens = ['expire', 'key', '15', 'LT']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.get_expiration('key') == initial_expiry

        # Try setting smaller expiration with LT
        ctx.cmdtokens = ['expire', 'key', '5', 'LT']
        cmd = ExpireCommand()
        result = cmd.execute(ctx)
        assert result == 1

    def test_expire_invalid_option_combinations(self, ctx):
        ctx.db.set('key', 'value')

        # Test NX and XX together
        ctx.cmdtokens = ['expire', 'key', '10', 'NX', 'XX']
        cmd = ExpireCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)

        # Test GT and LT together
        ctx.cmdtokens = ['expire', 'key', '10', 'GT', 'LT']
        cmd = ExpireCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)


class TestExpireatCommand:
    def test_expireat_basic(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        future_timestamp = int(time.time()) + 1
        ctx.cmdtokens = ['expireat', 'key', str(future_timestamp)]
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.exists_expiration('key')

        # time pass
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2
        assert ctx.db.get('key') is None

    def test_expireat_past_timestamp(self, ctx):
        ctx.db.set('key', 'value')
        past_timestamp = int(time.time()) - 1
        ctx.cmdtokens = ['expireat', 'key', str(past_timestamp)]
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('key') is None

    def test_expireat_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['expireat', 'nonexistent', str(int(time.time()))]
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_expireat_invalid_timestamp(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['expireat', 'key', 'invalid']
        cmd = ExpireatCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)

    def test_expireat_nx_option(self, ctx):
        ctx.db.set('key', 'value')
        future_timestamp = int(time.time()) + 10

        # Test NX when key has no expiration
        ctx.cmdtokens = ['expireat', 'key', str(future_timestamp), 'NX']
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 1

        # Test NX when key has expiration
        ctx.cmdtokens = ['expireat', 'key', str(future_timestamp + 10), 'NX']
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_expireat_xx_option(self, ctx):
        ctx.db.set('key', 'value')
        future_timestamp = int(time.time()) + 10

        # Test XX when key has no expiration
        ctx.cmdtokens = ['expireat', 'key', str(future_timestamp), 'XX']
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 0

        # Set expiration and try again
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))
        ctx.cmdtokens = ['expireat', 'key', str(future_timestamp), 'XX']
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 1

    def test_expireat_gt_option(self, ctx):
        ctx.db.set('key', 'value')
        initial_timestamp = int(time.time()) + 10
        ctx.db.set_expiration('key', initial_timestamp * 1000)

        # Try setting earlier timestamp with GT
        ctx.cmdtokens = ['expireat', 'key', str(initial_timestamp - 5), 'GT']
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 0

        # Try setting later timestamp with GT
        ctx.cmdtokens = ['expireat', 'key', str(initial_timestamp + 5), 'GT']
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 1

    def test_expireat_lt_option(self, ctx):
        ctx.db.set('key', 'value')
        initial_timestamp = int(time.time()) + 10
        ctx.db.set_expiration('key', initial_timestamp * 1000)

        # Try setting later timestamp with LT
        ctx.cmdtokens = ['expireat', 'key', str(initial_timestamp + 5), 'LT']
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 0

        # Try setting earlier timestamp with LT
        ctx.cmdtokens = ['expireat', 'key', str(initial_timestamp - 5), 'LT']
        cmd = ExpireatCommand()
        result = cmd.execute(ctx)
        assert result == 1

    def test_expireat_invalid_option_combinations(self, ctx):
        ctx.db.set('key', 'value')

        # Test NX and XX together
        ctx.cmdtokens = ['expireat', 'key', str(int(time.time()) + 10), 'NX', 'XX']
        cmd = ExpireatCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)

        # Test GT and LT together
        ctx.cmdtokens = ['expireat', 'key', str(int(time.time()) + 10), 'GT', 'LT']
        cmd = ExpireatCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)


class TestExpireTimeCommand:
    def test_expiretime_with_expiration(self, ctx):
        ctx.db.set('key', 'value')
        future_timestamp = int(time.time()) + 10
        ctx.db.set_expiration('key', future_timestamp * 1000)  # Convert to milliseconds

        ctx.cmdtokens = ['expiretime', 'key']
        cmd = ExpireTimeCommand()
        result = cmd.execute(ctx)
        assert result == future_timestamp

    def test_expiretime_no_expiration(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['expiretime', 'key']
        cmd = ExpireTimeCommand()
        result = cmd.execute(ctx)
        assert result == -1

    def test_expiretime_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['expiretime', 'nonexistent']
        cmd = ExpireTimeCommand()
        result = cmd.execute(ctx)
        assert result == -2

    def test_expiretime_after_expire(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        # Set expiration to 1 second from now
        future_timestamp = int(time.time()) + 1
        ctx.db.set_expiration('key', future_timestamp * 1000)

        # time pass
        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2
        ctx.cmdtokens = ['expiretime', 'key']
        cmd = ExpireTimeCommand()
        result = cmd.execute(ctx)
        assert result == -2  # Key should be expired and deleted


class TestIncrbyCommand:
    def test_incrby_existing_number(self, ctx):
        ctx.db.set('key', '10')
        ctx.cmdtokens = ['incrby', 'key', '5']
        cmd = IncrbyCommand()
        result = cmd.execute(ctx)
        assert result == '15'
        assert ctx.db.get('key') == '15'

    def test_incrby_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['incrby', 'key', '5']
        cmd = IncrbyCommand()
        result = cmd.execute(ctx)
        assert result == '5'
        assert ctx.db.get('key') == '5'

    def test_incrby_negative_number(self, ctx):
        ctx.db.set('key', '10')
        ctx.cmdtokens = ['incrby', 'key', '-5']
        cmd = IncrbyCommand()
        result = cmd.execute(ctx)
        assert result == '5'

    def test_incrby_non_integer_string(self, ctx):
        ctx.db.set('key', 'abc')
        ctx.cmdtokens = ['incrby', 'key', '5']
        cmd = IncrbyCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)

    def test_incrby_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        ctx.cmdtokens = ['incrby', 'key', '5']
        cmd = IncrbyCommand()
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestIncrbyfloatCommand:
    def test_incrbyfloat_existing_number(self, ctx):
        ctx.db.set('key', '10.5')
        ctx.cmdtokens = ['incrbyfloat', 'key', '2.5']
        cmd = IncrbyfloatCommand()
        result = cmd.execute(ctx)
        assert result == '13'
        assert ctx.db.get('key') == '13'

    def test_incrbyfloat_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['incrbyfloat', 'key', '5.5']
        cmd = IncrbyfloatCommand()
        result = cmd.execute(ctx)
        assert result == '5.5'
        assert ctx.db.get('key') == '5.5'

    def test_incrbyfloat_negative_number(self, ctx):
        ctx.db.set('key', '10.5')
        ctx.cmdtokens = ['incrbyfloat', 'key', '-2.5']
        cmd = IncrbyfloatCommand()
        result = cmd.execute(ctx)
        assert result == '8'

    def test_incrbyfloat_integer_value(self, ctx):
        ctx.db.set('key', '10')
        ctx.cmdtokens = ['incrbyfloat', 'key', '5.5']
        cmd = IncrbyfloatCommand()
        result = cmd.execute(ctx)
        assert result == '15.5'

    def test_incrbyfloat_non_numeric_string(self, ctx):
        ctx.db.set('key', 'abc')
        ctx.cmdtokens = ['incrbyfloat', 'key', '5.5']
        cmd = IncrbyfloatCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)

    def test_incrbyfloat_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        ctx.cmdtokens = ['incrbyfloat', 'key', '5.5']
        cmd = IncrbyfloatCommand()
        with pytest.raises(TypeError):
            cmd.execute(ctx)


class TestKeysCommand:
    def test_keys_exact_match(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.cmdtokens = ['keys', 'key1']
        cmd = KeysCommand()
        result = cmd.execute(ctx)
        assert result == ['key1']

    def test_keys_wildcard_all(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('other', 'value3')
        ctx.cmdtokens = ['keys', '*']
        cmd = KeysCommand()
        result = cmd.execute(ctx)
        assert sorted(result) == ['key1', 'key2', 'other']

    def test_keys_question_mark(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('key3', 'value3')
        ctx.cmdtokens = ['keys', 'key?']
        cmd = KeysCommand()
        result = cmd.execute(ctx)
        assert sorted(result) == ['key1', 'key2', 'key3']

    def test_keys_brackets(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('key3', 'value3')
        ctx.cmdtokens = ['keys', 'key[1-2]']
        cmd = KeysCommand()
        result = cmd.execute(ctx)
        assert sorted(result) == ['key1', 'key2']

    def test_keys_escaped_special_chars(self, ctx):
        ctx.db.set('key*1', 'value1')
        ctx.db.set('key?2', 'value2')
        ctx.cmdtokens = ['keys', 'key\\*1']
        cmd = KeysCommand()
        result = cmd.execute(ctx)
        assert result == ['key*1']

        ctx.cmdtokens = ['keys', 'key\\?2']
        cmd = KeysCommand()
        result = cmd.execute(ctx)
        assert result == ['key?2']

    def test_keys_no_match(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.cmdtokens = ['keys', 'nonexistent*']
        cmd = KeysCommand()
        result = cmd.execute(ctx)
        assert result == []


class TestMgetCommand:
    def test_mget_all_existing(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.cmdtokens = ['mget', 'key1', 'key2']
        cmd = MGetCommand()
        result = cmd.execute(ctx)
        assert result == ['value1', 'value2']

    def test_mget_some_missing(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.cmdtokens = ['mget', 'key1', 'nonexistent']
        cmd = MGetCommand()
        result = cmd.execute(ctx)
        assert result == ['value1', None]

    def test_mget_all_missing(self, ctx):
        ctx.cmdtokens = ['mget', 'nonexistent1', 'nonexistent2']
        cmd = MGetCommand()
        result = cmd.execute(ctx)
        assert result == [None, None]

    def test_mget_wrong_type(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', [1, 2, 3])  # Wrong type
        ctx.cmdtokens = ['mget', 'key1', 'key2']
        cmd = MGetCommand()
        result = cmd.execute(ctx)
        assert result[0] == 'value1'
        assert result[1] is not None  # The wrong type value should still be returned


class TestMsetCommand:
    def test_mset_basic(self, ctx):
        cmd = MSetCommand()
        ctx.cmdtokens = ['mset', 'key1', 'value1', 'key2', 'value2']
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key1') == 'value1'
        assert ctx.db.get('key2') == 'value2'

    def test_mset_override_existing(self, ctx):
        ctx.db.set('key1', 'oldvalue1')
        cmd = MSetCommand()
        ctx.cmdtokens = ['mset', 'key1', 'newvalue1', 'key2', 'value2']
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key1') == 'newvalue1'
        assert ctx.db.get('key2') == 'value2'

    def test_mset_invalid_pairs(self, ctx):
        ctx.cmdtokens = ['mset', 'key1', 'value1', 'key2']
        with pytest.raises(ValueError):
            MSetCommand().execute(ctx)  # Missing value for key2

    def test_mset_empty_values(self, ctx):
        ctx.cmdtokens = ['mset', 'key1', '', 'key2', '']
        cmd = MSetCommand()
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key1') == ''
        assert ctx.db.get('key2') == ''

    def test_mset_with_spaces(self, ctx):
        ctx.cmdtokens = ['mset', 'key 1', 'value 1', 'key 2', 'value 2']
        cmd = MSetCommand()
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.get('key 1') == 'value 1'
        assert ctx.db.get('key 2') == 'value 2'


class TestMsetnxCommand:
    def test_msetnx_all_new_keys(self, ctx):
        ctx.cmdtokens = ['msetnx', 'key1', 'value1', 'key2', 'value2']
        cmd = MSetnxCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('key1') == 'value1'
        assert ctx.db.get('key2') == 'value2'

    def test_msetnx_with_existing_key(self, ctx):
        ctx.db.set('key1', 'oldvalue')
        ctx.cmdtokens = ['msetnx', 'key1', 'value1', 'key2', 'value2']
        cmd = MSetnxCommand()
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.get('key1') == 'oldvalue'
        assert not ctx.db.exists('key2')

    def test_msetnx_invalid_pairs(self, ctx):
        ctx.cmdtokens = ['msetnx', 'key1']
        with pytest.raises(ValueError):
            MSetnxCommand().execute(ctx)  # Missing value

    def test_msetnx_empty_values(self, ctx):
        ctx.cmdtokens = ['msetnx', 'key1', '', 'key2', '']
        cmd = MSetnxCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('key1') == ''
        assert ctx.db.get('key2') == ''


class TestPersistCommand:
    def test_persist_with_expiration(self, ctx):
        # Set key with expiration
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))  # 5 seconds

        ctx.cmdtokens = ['persist', 'key']
        cmd = PersistCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists_expiration('key')
        assert ctx.db.get('key') == 'value'

    def test_persist_without_expiration(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['persist', 'key']
        cmd = PersistCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_persist_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['persist', 'nonexistent']
        cmd = PersistCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_persist_after_expiration(self, ctx, mock_time):
        # Set key with 1 second expiration
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 1000))

        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2

        ctx.cmdtokens = ['persist', 'key']
        cmd = PersistCommand()
        result = cmd.execute(ctx)
        assert result == 0
        assert not ctx.db.exists('key')


class TestRandomKeyCommand:
    def test_random_key_with_keys(self, ctx):
        ctx.db.set('key1', 'value1')
        ctx.db.set('key2', 'value2')
        ctx.db.set('key3', 'value3')

        ctx.cmdtokens = ['randomkey']
        cmd = RandomKeyCommand()
        result = cmd.execute(ctx)
        assert result in ['key1', 'key2', 'key3']

    def test_random_key_empty_db(self, ctx):
        ctx.cmdtokens = ['randomkey']
        cmd = RandomKeyCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_random_key_extra_args(self, ctx):
        ctx.cmdtokens = ['randomkey', 'extra']
        cmd = RandomKeyCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)


class TestRenameCommand:
    def test_rename_basic(self, ctx):
        ctx.db.set('old', 'value')
        ctx.cmdtokens = ['rename', 'old', 'new']
        cmd = RenameCommand()
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert not ctx.db.exists('old')
        assert ctx.db.get('new') == 'value'

    def test_rename_with_expiration(self, ctx):
        # Set key with expiration
        ctx.db.set('old', 'value')
        expiration = int(time.time() * 1000 + 5000)  # 5 seconds
        ctx.db.set_expiration('old', expiration)

        ctx.cmdtokens = ['rename', 'old', 'new']
        cmd = RenameCommand()
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert ctx.db.exists_expiration('new')
        assert ctx.db.get_expiration('new') == expiration

    def test_rename_overwrite_destination(self, ctx):
        ctx.db.set('old', 'value1')
        ctx.db.set('new', 'value2')
        ctx.cmdtokens = ['rename', 'old', 'new']
        cmd = RenameCommand()
        result = cmd.execute(ctx)
        assert result == 'OK'
        assert not ctx.db.exists('old')
        assert ctx.db.get('new') == 'value1'

    def test_rename_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['rename', 'nonexistent', 'new']
        cmd = RenameCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)

    def test_rename_same_key(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['rename', 'key', 'key']
        cmd = RenameCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)


class TestRenamenxCommand:
    def test_renamenx_basic(self, ctx):
        ctx.db.set('old', 'value')
        ctx.cmdtokens = ['renamenx', 'old', 'new']
        cmd = RenamenxCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists('old')
        assert ctx.db.get('new') == 'value'

    def test_renamenx_existing_destination(self, ctx):
        ctx.db.set('old', 'value1')
        ctx.db.set('new', 'value2')
        ctx.cmdtokens = ['renamenx', 'old', 'new']
        cmd = RenamenxCommand()
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.exists('old')
        assert ctx.db.get('new') == 'value2'

    def test_renamenx_with_expiration(self, ctx):
        # Set key with expiration
        ctx.db.set('old', 'value')
        expiration = int(time.time() * 1000 + 5000)  # 5 seconds
        ctx.db.set_expiration('old', expiration)

        ctx.cmdtokens = ['renamenx', 'old', 'new']
        cmd = RenamenxCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.exists_expiration('new')
        assert ctx.db.get_expiration('new') == expiration

    def test_renamenx_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['renamenx', 'nonexistent', 'new']
        with pytest.raises(ValueError):
            RenamenxCommand().execute(ctx)

    def test_renamenx_same_key(self, ctx):
        ctx.cmdtokens = ['renamenx', 'key1', 'key1']
        with pytest.raises(ValueError):
            RenamenxCommand().execute(ctx)


class TestStrlenCommand:
    def test_strlen_basic(self, ctx):
        ctx.db.set('key', 'hello')
        ctx.cmdtokens = ['strlen', 'key']
        cmd = StrlenCommand()
        result = cmd.execute(ctx)
        assert result == 5

    def test_strlen_empty_string(self, ctx):
        ctx.db.set('key', '')
        ctx.cmdtokens = ['strlen', 'key']
        cmd = StrlenCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_strlen_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['strlen', 'nonexistent']
        cmd = StrlenCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_strlen_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        ctx.cmdtokens = ['strlen', 'key']
        cmd = StrlenCommand()
        with pytest.raises(TypeError):
            cmd.execute(ctx)

    def test_strlen_unicode(self, ctx):
        ctx.db.set('key', '你好世界')  # Chinese characters
        ctx.cmdtokens = ['strlen', 'key']
        cmd = StrlenCommand()
        result = cmd.execute(ctx)
        assert result == 4


class TestSubstrCommand:
    def test_substr_basic(self, ctx):
        ctx.db.set('key', 'Hello World')
        ctx.cmdtokens = ['substr', 'key', '0', '4']
        cmd = SubstrCommand()
        result = cmd.execute(ctx)
        assert result == 'Hello'

    def test_substr_negative_indices(self, ctx):
        ctx.db.set('key', 'Hello World')
        ctx.cmdtokens = ['substr', 'key', '-5', '-1']
        cmd = SubstrCommand()
        result = cmd.execute(ctx)
        assert result == 'World'

    def test_substr_out_of_range(self, ctx):
        ctx.db.set('key', 'Hello')
        ctx.cmdtokens = ['substr', 'key', '0', '10']
        cmd = SubstrCommand()
        result = cmd.execute(ctx)
        assert result == 'Hello'

    def test_substr_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['substr', 'nonexistent', '0', '1']
        cmd = SubstrCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_substr_wrong_type(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        ctx.cmdtokens = ['substr', 'key', '0', '1']
        cmd = SubstrCommand()
        with pytest.raises(TypeError):
            cmd.execute(ctx)

    def test_substr_invalid_indices(self, ctx):
        ctx.db.set('key', 'Hello')
        ctx.cmdtokens = ['substr', 'key', 'invalid', '1']
        cmd = SubstrCommand()
        with pytest.raises(ValueError):
            cmd.execute(ctx)


class TestTTLCommand:
    def test_ttl_with_expiration(self, ctx):
        ctx.db.set('key', 'value')
        # Set expiration to 5 seconds from now
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))

        ctx.cmdtokens = ['ttl', 'key']
        cmd = TTLCommand()
        result = cmd.execute(ctx)
        assert 0 <= result <= 5

    def test_ttl_no_expiration(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['ttl', 'key']
        cmd = TTLCommand()
        result = cmd.execute(ctx)
        assert result == -1

    def test_ttl_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['ttl', 'nonexistent']
        cmd = TTLCommand()
        result = cmd.execute(ctx)
        assert result == -2

    def test_ttl_after_expire(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 1000))  # 1 second

        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2

        ctx.cmdtokens = ['ttl', 'key']
        cmd = TTLCommand()
        result = cmd.execute(ctx)
        assert result == -2


class TestPTTLCommand:
    def test_pttl_with_expiration(self, ctx):
        ctx.db.set('key', 'value')
        # Set expiration to 5 seconds from now
        ctx.db.set_expiration('key', int(time.time() * 1000 + 5000))

        ctx.cmdtokens = ['pttl', 'key']
        cmd = PTTLCommand()
        result = cmd.execute(ctx)
        assert 0 <= result <= 5000

    def test_pttl_no_expiration(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['pttl', 'key']
        cmd = PTTLCommand()
        result = cmd.execute(ctx)
        assert result == -1

    def test_pttl_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['pttl', 'nonexistent']
        cmd = PTTLCommand()
        result = cmd.execute(ctx)
        assert result == -2

    def test_pttl_after_expire(self, ctx, mock_time):
        ctx.db.set('key', 'value')
        ctx.db.set_expiration('key', int(time.time() * 1000 + 1000))  # 1 second

        mock_time.return_value = MOCK_TIME_INITIAL_TIMESTAMP + 2

        ctx.cmdtokens = ['pttl', 'key']
        cmd = PTTLCommand()
        result = cmd.execute(ctx)
        assert result == -2


class TestTypeCommand:
    def test_type_string(self, ctx):
        ctx.db.set('key', 'value')
        ctx.cmdtokens = ['type', 'key']
        cmd = TypeCommand()
        result = cmd.execute(ctx)
        assert result == 'string'

    def test_type_list(self, ctx):
        ctx.db.set('key', [1, 2, 3])
        ctx.cmdtokens = ['type', 'key']
        cmd = TypeCommand()
        result = cmd.execute(ctx)
        assert result == 'list'

    def test_type_hash(self, ctx):
        ctx.db.set('key', {'field': 'value'})
        ctx.cmdtokens = ['type', 'key']
        cmd = TypeCommand()
        result = cmd.execute(ctx)
        assert result == 'hash'

    def test_type_set(self, ctx):
        ctx.db.set('key', {1, 2, 3})
        ctx.cmdtokens = ['type', 'key']
        cmd = TypeCommand()
        result = cmd.execute(ctx)
        assert result == 'set'

    def test_type_zset(self, ctx):
        ctx.db.set('key', SortedSet({'member': 1.}))
        ctx.cmdtokens = ['type', 'key']
        cmd = TypeCommand()
        result = cmd.execute(ctx)
        assert result == 'zset'

    def test_type_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['type', 'nonexistent']
        cmd = TypeCommand()
        result = cmd.execute(ctx)
        assert result == 'none'
