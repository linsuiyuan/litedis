import pytest

from refactor2.core.command.base import CommandContext
from refactor2.core.command.listcmds import (
    LIndexCommand,
    LInsertCommand,
    LLenCommand,
    LPopCommand,
    LPushCommand, LPushXCommand, LRangeCommand, LRemCommand, LSetCommand, LTrimCommand, RPopCommand, RPushCommand,
    RPushXCommand, SortCommand,
)
from refactor2.core.persistence.ldb import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db)


class TestLIndexCommand:
    def test_lindex_empty_list(self, ctx):
        cmd = LIndexCommand(['lindex', 'mylist', '0'])
        assert cmd.execute(ctx) is None

    def test_lindex_valid_index(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LIndexCommand(['lindex', 'mylist', '1'])
        assert cmd.execute(ctx) == 'b'

    def test_lindex_negative_index(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LIndexCommand(['lindex', 'mylist', '-1'])
        assert cmd.execute(ctx) == 'c'

    def test_lindex_out_of_bounds(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LIndexCommand(['lindex', 'mylist', '5'])
        assert cmd.execute(ctx) is None

    def test_lindex_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        cmd = LIndexCommand(['lindex', 'mystr', '0'])
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLInsertCommand:
    def test_linsert_before(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LInsertCommand(['linsert', 'mylist', 'BEFORE', 'b', 'x'])
        assert cmd.execute(ctx) == 4
        assert ctx.db.get('mylist') == ['a', 'x', 'b', 'c']

    def test_linsert_after(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LInsertCommand(['linsert', 'mylist', 'AFTER', 'b', 'x'])
        assert cmd.execute(ctx) == 4
        assert ctx.db.get('mylist') == ['a', 'b', 'x', 'c']

    def test_linsert_pivot_not_found(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LInsertCommand(['linsert', 'mylist', 'BEFORE', 'z', 'x'])
        assert cmd.execute(ctx) == -1

    def test_linsert_key_not_exists(self, ctx):
        cmd = LInsertCommand(['linsert', 'mylist', 'BEFORE', 'b', 'x'])
        assert cmd.execute(ctx) == 0


class TestLLenCommand:
    def test_llen_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        cmd = LLenCommand(['llen', 'mylist'])
        assert cmd.execute(ctx) == 0

    def test_llen_populated_list(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LLenCommand(['llen', 'mylist'])
        assert cmd.execute(ctx) == 3

    def test_llen_key_not_exists(self, ctx):
        cmd = LLenCommand(['llen', 'mylist'])
        assert cmd.execute(ctx) == 0

    def test_llen_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        cmd = LLenCommand(['llen', 'mystr'])
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLPopCommand:
    def test_lpop_single_element(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LPopCommand(['lpop', 'mylist'])
        assert cmd.execute(ctx) == 'a'
        assert ctx.db.get('mylist') == ['b', 'c']

    def test_lpop_multiple_elements(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        cmd = LPopCommand(['lpop', 'mylist', '2'])
        assert cmd.execute(ctx) == ['a', 'b']
        assert ctx.db.get('mylist') == ['c', 'd']

    def test_lpop_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        cmd = LPopCommand(['lpop', 'mylist'])
        assert cmd.execute(ctx) is None

    def test_lpop_key_not_exists(self, ctx):
        cmd = LPopCommand(['lpop', 'mylist'])
        assert cmd.execute(ctx) is None

    def test_lpop_removes_empty_list(self, ctx):
        ctx.db.set('mylist', ['a'])
        cmd = LPopCommand(['lpop', 'mylist'])
        assert cmd.execute(ctx) == 'a'
        assert not ctx.db.exists('mylist')


class TestLPushCommand:
    def test_lpush_to_new_list(self, ctx):
        cmd = LPushCommand(['lpush', 'mylist', 'a', 'b'])
        assert cmd.execute(ctx) == 2
        assert ctx.db.get('mylist') == ['b', 'a']

    def test_lpush_to_existing_list(self, ctx):
        ctx.db.set('mylist', ['c'])
        cmd = LPushCommand(['lpush', 'mylist', 'a', 'b'])
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['b', 'a', 'c']

    def test_lpush_single_element(self, ctx):
        cmd = LPushCommand(['lpush', 'mylist', 'a'])
        assert cmd.execute(ctx) == 1
        assert ctx.db.get('mylist') == ['a']

    def test_lpush_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        cmd = LPushCommand(['lpush', 'mystr', 'a'])
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLPushXCommand:
    def test_lpushx_existing_list(self, ctx):
        ctx.db.set('mylist', ['c'])
        cmd = LPushXCommand(['lpushx', 'mylist', 'a', 'b'])
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['b', 'a', 'c']

    def test_lpushx_nonexistent_key(self, ctx):
        cmd = LPushXCommand(['lpushx', 'mylist', 'a', 'b'])
        assert cmd.execute(ctx) == 0
        assert not ctx.db.exists('mylist')

    def test_lpushx_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        cmd = LPushXCommand(['lpushx', 'mystr', 'a'])
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLRangeCommand:
    def test_lrange_full_list(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        cmd = LRangeCommand(['lrange', 'mylist', '0', '-1'])
        assert cmd.execute(ctx) == ['a', 'b', 'c', 'd']

    def test_lrange_partial_list(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        cmd = LRangeCommand(['lrange', 'mylist', '1', '2'])
        assert cmd.execute(ctx) == ['b', 'c']

    def test_lrange_negative_indices(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        cmd = LRangeCommand(['lrange', 'mylist', '-3', '-2'])
        assert cmd.execute(ctx) == ['b', 'c']

    def test_lrange_out_of_bounds(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LRangeCommand(['lrange', 'mylist', '5', '10'])
        assert cmd.execute(ctx) == []

    def test_lrange_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        cmd = LRangeCommand(['lrange', 'mylist', '0', '-1'])
        assert cmd.execute(ctx) == []

    def test_lrange_nonexistent_key(self, ctx):
        cmd = LRangeCommand(['lrange', 'mylist', '0', '-1'])
        assert cmd.execute(ctx) == []


class TestLRemCommand:
    def test_lrem_positive_count(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'a', 'c', 'a', 'd'])
        cmd = LRemCommand(['lrem', 'mylist', '2', 'a'])
        assert cmd.execute(ctx) == 2
        assert ctx.db.get('mylist') == ['b', 'c', 'a', 'd']

    def test_lrem_negative_count(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'a', 'c', 'a', 'd'])
        cmd = LRemCommand(['lrem', 'mylist', '-2', 'a'])
        assert cmd.execute(ctx) == 2
        assert ctx.db.get('mylist') == ['a', 'b', 'c', 'd']

    def test_lrem_zero_count(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'a', 'c', 'a', 'd'])
        cmd = LRemCommand(['lrem', 'mylist', '0', 'a'])
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['b', 'c', 'd']

    def test_lrem_element_not_found(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LRemCommand(['lrem', 'mylist', '1', 'x'])
        assert cmd.execute(ctx) == 0
        assert ctx.db.get('mylist') == ['a', 'b', 'c']

    def test_lrem_removes_empty_list(self, ctx):
        ctx.db.set('mylist', ['a', 'a', 'a'])
        cmd = LRemCommand(['lrem', 'mylist', '0', 'a'])
        assert cmd.execute(ctx) == 3
        assert not ctx.db.exists('mylist')


class TestLSetCommand:
    def test_lset_valid_index(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LSetCommand(['lset', 'mylist', '1', 'x'])
        assert cmd.execute(ctx) == "OK"
        assert ctx.db.get('mylist') == ['a', 'x', 'c']

    def test_lset_negative_index(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LSetCommand(['lset', 'mylist', '-1', 'x'])
        assert cmd.execute(ctx) == "OK"
        assert ctx.db.get('mylist') == ['a', 'b', 'x']

    def test_lset_out_of_range(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LSetCommand(['lset', 'mylist', '5', 'x'])
        with pytest.raises(ValueError, match="index out of range"):
            cmd.execute(ctx)

    def test_lset_nonexistent_key(self, ctx):
        cmd = LSetCommand(['lset', 'mylist', '0', 'x'])
        with pytest.raises(ValueError, match="no such key"):
            cmd.execute(ctx)

    def test_lset_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        cmd = LSetCommand(['lset', 'mystr', '0', 'x'])
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestLTrimCommand:
    def test_ltrim_keep_middle(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd', 'e'])
        cmd = LTrimCommand(['ltrim', 'mylist', '1', '3'])
        assert cmd.execute(ctx) == "OK"
        assert ctx.db.get('mylist') == ['b', 'c', 'd']

    def test_ltrim_negative_indices(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd', 'e'])
        cmd = LTrimCommand(['ltrim', 'mylist', '0', '-2'])
        assert cmd.execute(ctx) == "OK"
        assert ctx.db.get('mylist') == ['a', 'b', 'c', 'd']

    def test_ltrim_out_of_range(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LTrimCommand(['ltrim', 'mylist', '5', '10'])
        assert cmd.execute(ctx) == "OK"
        assert not ctx.db.exists('mylist')

    def test_ltrim_empty_result(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = LTrimCommand(['ltrim', 'mylist', '2', '1'])
        assert cmd.execute(ctx) == "OK"
        assert not ctx.db.exists('mylist')

    def test_ltrim_nonexistent_key(self, ctx):
        cmd = LTrimCommand(['ltrim', 'mylist', '0', '1'])
        assert cmd.execute(ctx) == "OK"
        assert not ctx.db.exists('mylist')


class TestRPopCommand:
    def test_rpop_single_element(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c'])
        cmd = RPopCommand(['rpop', 'mylist'])
        assert cmd.execute(ctx) == 'c'
        assert ctx.db.get('mylist') == ['a', 'b']

    def test_rpop_multiple_elements(self, ctx):
        ctx.db.set('mylist', ['a', 'b', 'c', 'd'])
        cmd = RPopCommand(['rpop', 'mylist', '2'])
        assert cmd.execute(ctx) == ['c', 'd']
        assert ctx.db.get('mylist') == ['a', 'b']

    def test_rpop_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        cmd = RPopCommand(['rpop', 'mylist'])
        assert cmd.execute(ctx) is None

    def test_rpop_key_not_exists(self, ctx):
        cmd = RPopCommand(['rpop', 'mylist'])
        assert cmd.execute(ctx) is None

    def test_rpop_removes_empty_list(self, ctx):
        ctx.db.set('mylist', ['a'])
        cmd = RPopCommand(['rpop', 'mylist'])
        assert cmd.execute(ctx) == 'a'
        assert not ctx.db.exists('mylist')

    def test_rpop_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        cmd = RPopCommand(['rpop', 'mystr'])
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestRPushCommand:
    def test_rpush_to_new_list(self, ctx):
        cmd = RPushCommand(['rpush', 'mylist', 'a', 'b'])
        assert cmd.execute(ctx) == 2
        assert ctx.db.get('mylist') == ['a', 'b']

    def test_rpush_to_existing_list(self, ctx):
        ctx.db.set('mylist', ['a'])
        cmd = RPushCommand(['rpush', 'mylist', 'b', 'c'])
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['a', 'b', 'c']

    def test_rpush_single_element(self, ctx):
        cmd = RPushCommand(['rpush', 'mylist', 'a'])
        assert cmd.execute(ctx) == 1
        assert ctx.db.get('mylist') == ['a']

    def test_rpush_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        cmd = RPushCommand(['rpush', 'mystr', 'a'])
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestRPushXCommand:
    def test_rpushx_existing_list(self, ctx):
        ctx.db.set('mylist', ['a'])
        cmd = RPushXCommand(['rpushx', 'mylist', 'b', 'c'])
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('mylist') == ['a', 'b', 'c']

    def test_rpushx_nonexistent_key(self, ctx):
        cmd = RPushXCommand(['rpushx', 'mylist', 'a', 'b'])
        assert cmd.execute(ctx) == 0
        assert not ctx.db.exists('mylist')

    def test_rpushx_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        cmd = RPushXCommand(['rpushx', 'mystr', 'a'])
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)


class TestSortCommand:
    def test_sort_numeric_asc(self, ctx):
        ctx.db.set('mylist', ['3', '1', '2'])
        cmd = SortCommand(['sort', 'mylist'])
        assert cmd.execute(ctx) == ['1', '2', '3']

    def test_sort_numeric_desc(self, ctx):
        ctx.db.set('mylist', ['3', '1', '2'])
        cmd = SortCommand(['sort', 'mylist', 'DESC'])
        assert cmd.execute(ctx) == ['3', '2', '1']

    def test_sort_alpha_asc(self, ctx):
        ctx.db.set('mylist', ['banana', 'apple', 'cherry'])
        cmd = SortCommand(['sort', 'mylist', 'ALPHA'])
        assert cmd.execute(ctx) == ['apple', 'banana', 'cherry']

    def test_sort_alpha_desc(self, ctx):
        ctx.db.set('mylist', ['banana', 'apple', 'cherry'])
        cmd = SortCommand(['sort', 'mylist', 'ALPHA', 'DESC'])
        assert cmd.execute(ctx) == ['cherry', 'banana', 'apple']

    def test_sort_with_store(self, ctx):
        ctx.db.set('mylist', ['3', '1', '2'])
        cmd = SortCommand(['sort', 'mylist', 'STORE', 'newlist'])
        assert cmd.execute(ctx) == 3
        assert ctx.db.get('newlist') == ['1', '2', '3']

    def test_sort_empty_list(self, ctx):
        ctx.db.set('mylist', [])
        cmd = SortCommand(['sort', 'mylist'])
        assert cmd.execute(ctx) == []

    def test_sort_nonexistent_key(self, ctx):
        cmd = SortCommand(['sort', 'mylist'])
        assert cmd.execute(ctx) == []

    def test_sort_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_list')
        cmd = SortCommand(['sort', 'mystr'])
        with pytest.raises(TypeError, match="value is not a list"):
            cmd.execute(ctx)

    def test_sort_non_numeric_values(self, ctx):
        ctx.db.set('mylist', ['abc', 'def', '123'])
        cmd = SortCommand(['sort', 'mylist'])
        with pytest.raises(ValueError, match="one or more elements can't be converted to number"):
            cmd.execute(ctx)
