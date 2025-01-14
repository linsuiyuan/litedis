import pytest

from refactor2.core.command.base import CommandContext
from refactor2.core.command.listcmds import (
    LIndexCommand,
    LInsertCommand,
    LLenCommand,
    LPopCommand,
    LPushCommand,
    LPushXCommand,
    LRangeCommand,
    LRemCommand,
    LSetCommand,
    LTrimCommand,
    RPopCommand,
    RPushCommand,
    RPushXCommand,
    SortCommand,
)
from refactor2.core.persistence.ldb import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db, [])


class TestLIndexCommand:
    def test_lindex_empty_list(self, ctx):
        ctx.cmdtokens = ['lindex', 'mylist', '0']
        cmd = LIndexCommand()
        assert cmd.execute(ctx) is None

    def test_lindex_valid_index(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['lindex', 'mylist', '1']
        cmd = LIndexCommand()
        assert cmd.execute(ctx) == 'b'

    def test_lindex_negative_index(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['lindex', 'mylist', '-1']
        cmd = LIndexCommand()
        assert cmd.execute(ctx) == 'c'

    def test_lindex_out_of_bounds(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['lindex', 'mylist', '5']
        cmd = LIndexCommand()
        assert cmd.execute(ctx) is None

    def test_lindex_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        ctx.cmdtokens = ['lindex', 'mystr', '0']
        cmd = LIndexCommand()
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLInsertCommand:
    def test_linsert_before(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['linsert', 'mylist', 'BEFORE', 'b', 'x']
        cmd = LInsertCommand()
        assert cmd.execute(ctx) == 4
        assert ctx.db.get('mylist') == ['a', 'x', 'b', 'c']

    def test_linsert_after(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['linsert', 'mylist', 'AFTER', 'b', 'x']
        cmd = LInsertCommand()
        assert cmd.execute(ctx) == 4
        assert ctx.db.get('mylist') == ['a', 'b', 'x', 'c']

    def test_linsert_pivot_not_found(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['linsert', 'mylist', 'BEFORE', 'z', 'x']
        cmd = LInsertCommand()
        assert cmd.execute(ctx) == -1

    def test_linsert_key_not_exists(self, ctx):
        ctx.cmdtokens = ['linsert', 'mylist', 'BEFORE', 'b', 'x']
        cmd = LInsertCommand()
        assert cmd.execute(ctx) == 0


class TestLLenCommand:
    def test_llen_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        ctx.cmdtokens = ['llen', 'mylist']
        cmd = LLenCommand()
        assert cmd.execute(ctx) == 0

    def test_llen_populated_list(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['llen', 'mylist']
        cmd = LLenCommand()
        assert cmd.execute(ctx) == 3

    def test_llen_key_not_exists(self, ctx):
        ctx.cmdtokens = ['llen', 'mylist']
        cmd = LLenCommand()
        assert cmd.execute(ctx) == 0

    def test_llen_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        ctx.cmdtokens = ['llen', 'mystr']
        cmd = LLenCommand()
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLPopCommand:
    def test_lpop_single_element(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['lpop', 'mylist']
        cmd = LPopCommand()
        assert cmd.execute(ctx) == 'a'
        assert ctx.db.get('mylist') == ['b', 'c']

    def test_lpop_multiple_elements(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        ctx.cmdtokens = ['lpop', 'mylist', '2']
        cmd = LPopCommand()
        assert cmd.execute(ctx) == ['a', 'b']
        assert ctx.db.get('mylist') == ['c', 'd']

    def test_lpop_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        ctx.cmdtokens = ['lpop', 'mylist']
        cmd = LPopCommand()
        assert cmd.execute(ctx) is None

    def test_lpop_key_not_exists(self, ctx):
        ctx.cmdtokens = ['lpop', 'mylist']
        cmd = LPopCommand()
        assert cmd.execute(ctx) is None

    def test_lpop_removes_empty_list(self, ctx):
        ctx.db.set('mylist', ['a'])
        ctx.cmdtokens = ['lpop', 'mylist']
        cmd = LPopCommand()
        assert cmd.execute(ctx) == 'a'
        assert not ctx.db.exists('mylist')


class TestLPushCommand:
    def test_lpush_to_new_list(self, ctx):
        ctx.cmdtokens = ['lpush', 'mylist', 'a', 'b']
        cmd = LPushCommand()
        assert cmd.execute(ctx) == 2
        assert ctx.db.get('mylist') == ['b', 'a']

    def test_lpush_to_existing_list(self, ctx):
        ctx.db.set('mylist', ['c'])
        ctx.cmdtokens = ['lpush', 'mylist', 'a', 'b']
        cmd = LPushCommand()
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['b', 'a', 'c']

    def test_lpush_single_element(self, ctx):
        ctx.cmdtokens = ['lpush', 'mylist', 'a']
        cmd = LPushCommand()
        assert cmd.execute(ctx) == 1
        assert ctx.db.get('mylist') == ['a']

    def test_lpush_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        ctx.cmdtokens = ['lpush', 'mystr', 'a']
        cmd = LPushCommand()
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLPushXCommand:
    def test_lpushx_existing_list(self, ctx):
        ctx.db.set('mylist', ['c'])
        ctx.cmdtokens = ['lpushx', 'mylist', 'a', 'b']
        cmd = LPushXCommand()
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['b', 'a', 'c']

    def test_lpushx_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['lpushx', 'mylist', 'a', 'b']
        cmd = LPushXCommand()
        assert cmd.execute(ctx) == 0
        assert not ctx.db.exists('mylist')

    def test_lpushx_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        ctx.cmdtokens = ['lpushx', 'mystr', 'a']
        cmd = LPushXCommand()
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLRangeCommand:
    def test_lrange_full_list(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        ctx.cmdtokens = ['lrange', 'mylist', '0', '-1']
        cmd = LRangeCommand()
        assert cmd.execute(ctx) == ['a', 'b', 'c', 'd']

    def test_lrange_partial_list(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        ctx.cmdtokens = ['lrange', 'mylist', '1', '2']
        cmd = LRangeCommand()
        assert cmd.execute(ctx) == ['b', 'c']

    def test_lrange_negative_indices(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        ctx.cmdtokens = ['lrange', 'mylist', '-3', '-2']
        cmd = LRangeCommand()
        assert cmd.execute(ctx) == ['b', 'c']

    def test_lrange_out_of_bounds(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['lrange', 'mylist', '5', '10']
        cmd = LRangeCommand()
        assert cmd.execute(ctx) == []

    def test_lrange_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        ctx.cmdtokens = ['lrange', 'mylist', '0', '-1']
        cmd = LRangeCommand()
        assert cmd.execute(ctx) == []

    def test_lrange_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['lrange', 'mylist', '0', '-1']
        cmd = LRangeCommand()
        assert cmd.execute(ctx) == []


class TestLRemCommand:
    def test_lrem_positive_count(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'a', 'c', 'a', 'd'])
        ctx.cmdtokens = ['lrem', 'mylist', '2', 'a']
        cmd = LRemCommand()
        assert cmd.execute(ctx) == 2
        assert ctx.db.get('mylist') == ['b', 'c', 'a', 'd']

    def test_lrem_negative_count(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'a', 'c', 'a', 'd'])
        ctx.cmdtokens = ['lrem', 'mylist', '-2', 'a']
        cmd = LRemCommand()
        assert cmd.execute(ctx) == 2
        assert ctx.db.get('mylist') == ['a', 'b', 'c', 'd']

    def test_lrem_zero_count(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'a', 'c', 'a', 'd'])
        ctx.cmdtokens = ['lrem', 'mylist', '0', 'a']
        cmd = LRemCommand()
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['b', 'c', 'd']

    def test_lrem_element_not_found(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['lrem', 'mylist', '1', 'x']
        cmd = LRemCommand()
        assert cmd.execute(ctx) == 0
        assert ctx.db.get('mylist') == ['a', 'b', 'c']

    def test_lrem_removes_empty_list(self, ctx):
        ctx.db.set('mylist', ['a', 'a', 'a'])
        ctx.cmdtokens = ['lrem', 'mylist', '0', 'a']
        cmd = LRemCommand()
        assert cmd.execute(ctx) == 3
        assert not ctx.db.exists('mylist')


class TestLSetCommand:
    def test_lset_valid_index(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['lset', 'mylist', '1', 'x']
        cmd = LSetCommand()
        assert cmd.execute(ctx) == "OK"
        assert ctx.db.get('mylist') == ['a', 'x', 'c']

    def test_lset_negative_index(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['lset', 'mylist', '-1', 'x']
        cmd = LSetCommand()
        assert cmd.execute(ctx) == "OK"
        assert ctx.db.get('mylist') == ['a', 'b', 'x']

    def test_lset_out_of_range(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['lset', 'mylist', '5', 'x']
        cmd = LSetCommand()
        with pytest.raises(ValueError, match="index out of range"):
            cmd.execute(ctx)

    def test_lset_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['lset', 'mylist', '0', 'x']
        cmd = LSetCommand()
        with pytest.raises(ValueError, match="no such key"):
            cmd.execute(ctx)

    def test_lset_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        ctx.cmdtokens = ['lset', 'mystr', '0', 'x']
        cmd = LSetCommand()
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLTrimCommand:
    def test_ltrim_keep_middle(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd', 'e'])
        ctx.cmdtokens = ['ltrim', 'mylist', '1', '3']
        cmd = LTrimCommand()
        assert cmd.execute(ctx) == "OK"
        assert ctx.db.get('mylist') == ['b', 'c', 'd']

    def test_ltrim_negative_indices(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd', 'e'])
        ctx.cmdtokens = ['ltrim', 'mylist', '0', '-2']
        cmd = LTrimCommand()
        assert cmd.execute(ctx) == "OK"
        assert ctx.db.get('mylist') == ['a', 'b', 'c', 'd']

    def test_ltrim_out_of_range(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['ltrim', 'mylist', '5', '10']
        cmd = LTrimCommand()
        assert cmd.execute(ctx) == "OK"
        assert not ctx.db.exists('mylist')

    def test_ltrim_empty_result(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['ltrim', 'mylist', '2', '1']
        cmd = LTrimCommand()
        assert cmd.execute(ctx) == "OK"
        assert not ctx.db.exists('mylist')

    def test_ltrim_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['ltrim', 'mylist', '0', '1']
        cmd = LTrimCommand()
        assert cmd.execute(ctx) == "OK"
        assert not ctx.db.exists('mylist')


class TestRPopCommand:
    def test_rpop_single_element(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        ctx.cmdtokens = ['rpop', 'mylist']
        cmd = RPopCommand()
        assert cmd.execute(ctx) == 'c'
        assert ctx.db.get('mylist') == ['a', 'b']

    def test_rpop_multiple_elements(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        ctx.cmdtokens = ['rpop', 'mylist', '2']
        cmd = RPopCommand()
        assert cmd.execute(ctx) == ['d', 'c']
        assert ctx.db.get('mylist') == ['a', 'b']

    def test_rpop_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        ctx.cmdtokens = ['rpop', 'mylist']
        cmd = RPopCommand()
        assert cmd.execute(ctx) is None

    def test_rpop_key_not_exists(self, ctx):
        ctx.cmdtokens = ['rpop', 'mylist']
        cmd = RPopCommand()
        assert cmd.execute(ctx) is None

    def test_rpop_removes_empty_list(self, ctx):
        ctx.db.set('mylist', ['a'])
        ctx.cmdtokens = ['rpop', 'mylist']
        cmd = RPopCommand()
        assert cmd.execute(ctx) == 'a'
        assert not ctx.db.exists('mylist')

    def test_rpop_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        ctx.cmdtokens = ['rpop', 'mystr']
        cmd = RPopCommand()
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestRPushCommand:
    def test_rpush_to_new_list(self, ctx):
        ctx.cmdtokens = ['rpush', 'mylist', 'a', 'b']
        cmd = RPushCommand()
        assert cmd.execute(ctx) == 2
        assert ctx.db.get('mylist') == ['a', 'b']

    def test_rpush_to_existing_list(self, ctx):
        ctx.db.set('mylist', ['a'])
        ctx.cmdtokens = ['rpush', 'mylist', 'b', 'c']
        cmd = RPushCommand()
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['a', 'b', 'c']

    def test_rpush_single_element(self, ctx):
        ctx.cmdtokens = ['rpush', 'mylist', 'a']
        cmd = RPushCommand()
        assert cmd.execute(ctx) == 1
        assert ctx.db.get('mylist') == ['a']

    def test_rpush_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        ctx.cmdtokens = ['rpush', 'mystr', 'a']
        cmd = RPushCommand()
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestRPushXCommand:
    def test_rpushx_existing_list(self, ctx):
        ctx.db.set('mylist', ['a'])
        ctx.cmdtokens = ['rpushx', 'mylist', 'b', 'c']
        cmd = RPushXCommand()
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['a', 'b', 'c']

    def test_rpushx_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['rpushx', 'mylist', 'a', 'b']
        cmd = RPushXCommand()
        assert cmd.execute(ctx) == 0
        assert not ctx.db.exists('mylist')

    def test_rpushx_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        ctx.cmdtokens = ['rpushx', 'mystr', 'a']
        cmd = RPushXCommand()
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestSortCommand:
    def test_sort_numeric_asc(self, ctx):
        ctx.db.set('mylist', ['3', '1', '2'])
        ctx.cmdtokens = ['sort', 'mylist']
        cmd = SortCommand()
        assert cmd.execute(ctx) == ['1', '2', '3']

    def test_sort_numeric_desc(self, ctx):
        ctx.db.set('mylist', ['3', '1', '2'])
        ctx.cmdtokens = ['sort', 'mylist', 'DESC']
        cmd = SortCommand()
        assert cmd.execute(ctx) == ['3', '2', '1']

    def test_sort_alpha_asc(self, ctx):
        ctx.db.set('mylist', ['banana', 'apple', 'cherry'])
        ctx.cmdtokens = ['sort', 'mylist', 'ALPHA']
        cmd = SortCommand()
        assert cmd.execute(ctx) == ['apple', 'banana', 'cherry']

    def test_sort_alpha_desc(self, ctx):
        ctx.db.set('mylist', ['banana', 'apple', 'cherry'])
        ctx.cmdtokens = ['sort', 'mylist', 'ALPHA', 'DESC']
        cmd = SortCommand()
        assert cmd.execute(ctx) == ['cherry', 'banana', 'apple']

    def test_sort_with_store(self, ctx):
        ctx.db.set('mylist', ['3', '1', '2'])
        ctx.cmdtokens = ['sort', 'mylist', 'STORE', 'newlist']
        cmd = SortCommand()
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('newlist') == ['1', '2', '3']

    def test_sort_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        ctx.cmdtokens = ['sort', 'mylist']
        cmd = SortCommand()
        assert cmd.execute(ctx) == []

    def test_sort_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['sort', 'mylist']
        cmd = SortCommand()
        assert cmd.execute(ctx) == []

    def test_sort_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        ctx.cmdtokens = ['sort', 'mystr']
        cmd = SortCommand()
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)

    def test_sort_non_numeric_values(self, ctx):
        ctx.db.set('mylist', ['abc', 'def', '123'])
        ctx.cmdtokens = ['sort', 'mylist']
        cmd = SortCommand()
        with pytest.raises(ValueError, match="one or more elements can't be converted to number"):
            cmd.execute(ctx)
