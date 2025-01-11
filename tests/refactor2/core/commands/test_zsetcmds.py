import pytest

from refactor2.core.command.base import CommandContext
from refactor2.core.command.sortedset import SortedSet
from refactor2.core.command.zsetcmds import (
    ZAddCommand,
    ZCardCommand,
    ZCountCommand,
    ZDiffCommand,
    ZIncrByCommand,
    ZInterCommand,
    ZInterCardCommand,
    ZInterStoreCommand,
    ZPopMaxCommand,
    ZPopMinCommand,
    ZRandMemberCommand,
    ZMPopCommand,
    ZRangeCommand,
    ZRangeByScoreCommand,
    ZRevRangeByScoreCommand,
    ZRankCommand,
    ZRemCommand,
    ZRemRangeByScoreCommand,
    ZRevRankCommand,
    ZScanCommand,
    ZScoreCommand,
    ZUnionCommand,
    ZMScoreCommand,
)
from refactor2.core.persistence.ldb import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db)


class TestZAddCommand:
    def test_zadd_new_key(self, ctx):
        cmd = ZAddCommand(['zadd', 'myset', '1.5', 'member1', '2.0', 'member2'])
        result = cmd.execute(ctx)

        assert result == 2
        assert isinstance(ctx.db.get('myset'), SortedSet)
        assert ctx.db.get('myset').score('member1') == 1.5
        assert ctx.db.get('myset').score('member2') == 2.0

    def test_zadd_existing_members(self, ctx):
        # First add
        cmd1 = ZAddCommand(['zadd', 'myset', '1.5', 'member1'])
        cmd1.execute(ctx)

        # Update existing member
        cmd2 = ZAddCommand(['zadd', 'myset', '2.0', 'member1', '3.0', 'member2'])
        result = cmd2.execute(ctx)

        assert result == 1  # Only member2 is new
        assert ctx.db.get('myset').score('member1') == 2.0
        assert ctx.db.get('myset').score('member2') == 3.0

    def test_zadd_invalid_score(self, ctx):
        with pytest.raises(ValueError, match='invalid score'):
            ZAddCommand(['zadd', 'myset', 'notanumber', 'member1'])

    def test_zadd_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zadd command requires key, score and member'):
            ZAddCommand(['zadd', 'myset'])

        with pytest.raises(ValueError, match='score and member must come in pairs'):
            ZAddCommand(['zadd', 'myset', '1.5', 'member1', '2.0'])


class TestZCardCommand:
    def test_zcard_empty_key(self, ctx):
        cmd = ZCardCommand(['zcard', 'myset'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_zcard_with_members(self, ctx):
        # Add some members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZCardCommand(['zcard', 'myset'])
        result = cmd.execute(ctx)
        assert result == 2

    def test_zcard_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZCardCommand(['zcard', 'myset'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zcard_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zcard command requires key'):
            ZCardCommand(['zcard'])


class TestZCountCommand:
    def test_zcount_empty_key(self, ctx):
        cmd = ZCountCommand(['zcount', 'myset', '0', '10'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_zcount_with_members(self, ctx):
        # Add members with different scores
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZCountCommand(['zcount', 'myset', '1.5', '3.0'])
        result = cmd.execute(ctx)
        assert result == 2  # member2 and member3

    def test_zcount_invalid_range(self, ctx):
        with pytest.raises(ValueError, match='min and max must be valid float numbers'):
            ZCountCommand(['zcount', 'myset', 'notanumber', '10'])

    def test_zcount_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zcount command requires key, min and max'):
            ZCountCommand(['zcount', 'myset'])


class TestZDiffCommand:
    def test_zdiff_empty_keys(self, ctx):
        cmd = ZDiffCommand(['zdiff', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert result == []

    def test_zdiff_with_members(self, ctx):
        # Add members to first set
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd1.execute(ctx)

        # Add members to second set
        zadd2 = ZAddCommand(['zadd', 'set2', '2.0', 'member2', '4.0', 'member4'])
        zadd2.execute(ctx)

        cmd = ZDiffCommand(['zdiff', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert 'member1' in result
        assert 'member3' in result

    def test_zdiff_wrong_type(self, ctx):
        ctx.db.set('set1', "string")  # Wrong type
        cmd = ZDiffCommand(['zdiff', '2', 'set1', 'set2'])
        with pytest.raises(TypeError, match="value at set1 is not a sorted set"):
            cmd.execute(ctx)

    def test_zdiff_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zdiff command requires numkeys and at least one key'):
            ZDiffCommand(['zdiff'])

        with pytest.raises(ValueError, match='numkeys must be positive'):
            ZDiffCommand(['zdiff', '-1', 'set1'])

    def test_zdiff_with_scores(self, ctx):
        # Add members to first set
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd1.execute(ctx)

        # Add members to second set
        zadd2 = ZAddCommand(['zadd', 'set2', '2.0', 'member2', '4.0', 'member4'])
        zadd2.execute(ctx)

        cmd = ZDiffCommand(['zdiff', '2', 'set1', 'set2', 'WITHSCORES'])
        result = cmd.execute(ctx)

        # Convert result to dict for easier comparison
        scores = {result[i]: result[i + 1] for i in range(0, len(result), 2)}
        assert len(scores) == 2
        assert scores['member1'] == 1.0
        assert scores['member3'] == 3.0


class TestZIncrByCommand:
    def test_zincrby_new_member(self, ctx):
        cmd = ZIncrByCommand(['zincrby', 'myset', '1.5', 'member1'])
        result = cmd.execute(ctx)
        assert result == 1.5
        assert ctx.db.get('myset').score('member1') == 1.5

    def test_zincrby_existing_member(self, ctx):
        # First add member
        zadd = ZAddCommand(['zadd', 'myset', '2.0', 'member1'])
        zadd.execute(ctx)

        # Increment score
        cmd = ZIncrByCommand(['zincrby', 'myset', '1.5', 'member1'])
        result = cmd.execute(ctx)
        assert result == 3.5
        assert ctx.db.get('myset').score('member1') == 3.5

    def test_zincrby_invalid_increment(self, ctx):
        with pytest.raises(ValueError, match='increment must be a valid float number'):
            ZIncrByCommand(['zincrby', 'myset', 'notanumber', 'member1'])

    def test_zincrby_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zincrby command requires key, increment and member'):
            ZIncrByCommand(['zincrby', 'myset'])


class TestZInterCommand:
    def test_zinter_empty_keys(self, ctx):
        cmd = ZInterCommand(['zinter', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert result == []

    def test_zinter_with_members(self, ctx):
        # Add members to first set
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd1.execute(ctx)

        # Add members to second set
        zadd2 = ZAddCommand(['zadd', 'set2', '2.0', 'member2', '3.0', 'member3', '4.0', 'member4'])
        zadd2.execute(ctx)

        cmd = ZInterCommand(['zinter', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert 'member2' in result
        assert 'member3' in result

    def test_zinter_with_withscores(self, ctx):
        # Add members to sets
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2'])
        zadd1.execute(ctx)
        zadd2 = ZAddCommand(['zadd', 'set2', '2.0', 'member2', '3.0', 'member3'])
        zadd2.execute(ctx)

        cmd = ZInterCommand(['zinter', '2', 'set1', 'set2', 'WITHSCORES'])
        result = cmd.execute(ctx)
        assert result == ['member2', 2.0]

    def test_zinter_wrong_type(self, ctx):
        ctx.db.set('set1', "string")  # Wrong type
        cmd = ZInterCommand(['zinter', '2', 'set1', 'set2'])
        with pytest.raises(TypeError, match="value at set1 is not a sorted set"):
            cmd.execute(ctx)

    def test_zinter_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zinter command requires numkeys and at least one key'):
            ZInterCommand(['zinter'])

        with pytest.raises(ValueError, match='numkeys must be positive'):
            ZInterCommand(['zinter', '-1', 'set1'])


class TestZInterCardCommand:
    def test_zintercard_empty_keys(self, ctx):
        cmd = ZInterCardCommand(['zintercard', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_zintercard_with_members(self, ctx):
        # Add members to sets
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd1.execute(ctx)
        zadd2 = ZAddCommand(['zadd', 'set2', '2.0', 'member2', '3.0', 'member3', '4.0', 'member4'])
        zadd2.execute(ctx)

        cmd = ZInterCardCommand(['zintercard', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert result == 2  # member2 and member3

    def test_zintercard_with_limit(self, ctx):
        # Add members to sets
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd1.execute(ctx)
        zadd2 = ZAddCommand(['zadd', 'set2', '2.0', 'member2', '3.0', 'member3', '4.0', 'member4'])
        zadd2.execute(ctx)

        cmd = ZInterCardCommand(['zintercard', '2', 'set1', 'set2', 'LIMIT', '1'])
        result = cmd.execute(ctx)
        assert result == 1

    def test_zintercard_wrong_type(self, ctx):
        ctx.db.set('set1', "string")  # Wrong type
        cmd = ZInterCardCommand(['zintercard', '2', 'set1', 'set2'])
        with pytest.raises(TypeError, match="value at set1 is not a sorted set"):
            cmd.execute(ctx)

    def test_zintercard_invalid_limit(self, ctx):
        with pytest.raises(ValueError, match='limit must be non-negative'):
            ZInterCardCommand(['zintercard', '2', 'set1', 'set2', 'LIMIT', '-1'])


class TestZInterStoreCommand:
    def test_zinterstore_empty_keys(self, ctx):
        cmd = ZInterStoreCommand(['zinterstore', 'dest', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert result == 0
        assert isinstance(ctx.db.get('dest'), SortedSet)
        assert len(ctx.db.get('dest')) == 0

    def test_zinterstore_with_members(self, ctx):
        # Add members to sets
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd1.execute(ctx)
        zadd2 = ZAddCommand(['zadd', 'set2', '2.0', 'member2', '3.0', 'member3', '4.0', 'member4'])
        zadd2.execute(ctx)

        cmd = ZInterStoreCommand(['zinterstore', 'dest', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert result == 2

        dest_set = ctx.db.get('dest')
        assert dest_set.score('member2') == 2.0
        assert dest_set.score('member3') == 3.0

    def test_zinterstore_wrong_type(self, ctx):
        ctx.db.set('set1', "string")  # Wrong type
        cmd = ZInterStoreCommand(['zinterstore', 'dest', '2', 'set1', 'set2'])
        with pytest.raises(TypeError, match="value at set1 is not a sorted set"):
            cmd.execute(ctx)

    def test_zinterstore_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zinterstore command requires destination, numkeys and at least one key'):
            ZInterStoreCommand(['zinterstore'])


class TestZPopMaxCommand:
    def test_zpopmax_empty_key(self, ctx):
        cmd = ZPopMaxCommand(['zpopmax', 'myset'])
        result = cmd.execute(ctx)
        assert result == []

    def test_zpopmax_single_member(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZPopMaxCommand(['zpopmax', 'myset'])
        result = cmd.execute(ctx)
        assert len(result) == 1
        assert result[0] == ('member3', 3.0)

        # Verify member was removed
        assert ctx.db.get('myset').score('member3') is None

    def test_zpopmax_multiple_members(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZPopMaxCommand(['zpopmax', 'myset', '2'])
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert result[0] == ('member3', 3.0)
        assert result[1] == ('member2', 2.0)

        # Verify members were removed
        zset = ctx.db.get('myset')
        assert len(zset) == 1
        assert zset.score('member1') == 1.0

    def test_zpopmax_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZPopMaxCommand(['zpopmax', 'myset'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)


class TestZPopMinCommand:
    def test_zpopmin_empty_key(self, ctx):
        cmd = ZPopMinCommand(['zpopmin', 'myset'])
        result = cmd.execute(ctx)
        assert result == []

    def test_zpopmin_single_member(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZPopMinCommand(['zpopmin', 'myset'])
        result = cmd.execute(ctx)
        assert len(result) == 1
        assert result[0] == ('member1', 1.0)

        # Verify member was removed
        assert ctx.db.get('myset').score('member1') is None

    def test_zpopmin_multiple_members(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZPopMinCommand(['zpopmin', 'myset', '2'])
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert result[0] == ('member1', 1.0)
        assert result[1] == ('member2', 2.0)

        # Verify members were removed
        zset = ctx.db.get('myset')
        assert len(zset) == 1
        assert zset.score('member3') == 3.0

    def test_zpopmin_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZPopMinCommand(['zpopmin', 'myset'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)


class TestZRandMemberCommand:
    def test_zrandmember_empty_key(self, ctx):
        cmd = ZRandMemberCommand(['zrandmember', 'myset'])
        result = cmd.execute(ctx)
        assert result is None

    def test_zrandmember_single_member(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRandMemberCommand(['zrandmember', 'myset'])
        result = cmd.execute(ctx)
        assert isinstance(result, str)
        assert result in ['member1', 'member2', 'member3']

    def test_zrandmember_multiple_members(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRandMemberCommand(['zrandmember', 'myset', '2'])
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert all(member in ['member1', 'member2', 'member3'] for member in result)

    def test_zrandmember_with_scores(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRandMemberCommand(['zrandmember', 'myset', '1', 'WITHSCORES'])
        result = cmd.execute(ctx)
        assert len(result) == 2  # [member, score]
        assert result[0] in ['member1', 'member2']
        assert result[1] in [1.0, 2.0]

    def test_zrandmember_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZRandMemberCommand(['zrandmember', 'myset'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zrandmember_invalid_count(self, ctx):
        with pytest.raises(ValueError, match='count must be an integer'):
            ZRandMemberCommand(['zrandmember', 'myset', 'notanumber'])


class TestZMPopCommand:
    def test_zmpop_empty_key(self, ctx):
        cmd = ZMPopCommand(['zmpop', '1', 'myset', 'MIN'])
        result = cmd.execute(ctx)
        assert result is None

    def test_zmpop_single_key_min(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZMPopCommand(['zmpop', '1', 'myset', 'MIN'])
        result = cmd.execute(ctx)
        assert result == ['myset', [('member1', 1.0)]]

    def test_zmpop_single_key_max(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZMPopCommand(['zmpop', '1', 'myset', 'MAX'])
        result = cmd.execute(ctx)
        assert result == ['myset', [('member2', 2.0)]]

    def test_zmpop_multiple_keys(self, ctx):
        # Add members to different sets
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1'])
        zadd1.execute(ctx)
        zadd2 = ZAddCommand(['zadd', 'set2', '2.0', 'member2'])
        zadd2.execute(ctx)

        cmd = ZMPopCommand(['zmpop', '2', 'set1', 'set2', 'MIN'])
        result = cmd.execute(ctx)
        assert result == ['set1', [('member1', 1.0)]]

    def test_zmpop_with_count(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZMPopCommand(['zmpop', '1', 'myset', 'MIN', 'COUNT', '2'])
        result = cmd.execute(ctx)
        assert result == ['myset', [('member1', 1.0), ('member2', 2.0)]]

    def test_zmpop_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZMPopCommand(['zmpop', '1', 'myset', 'MIN'])
        with pytest.raises(TypeError, match="value at myset is not a sorted set"):
            cmd.execute(ctx)

    def test_zmpop_invalid_args(self, ctx):
        with pytest.raises(ValueError, match='WHERE must be either MIN or MAX'):
            ZMPopCommand(['zmpop', '1', 'myset', 'INVALID'])


class TestZRangeCommand:
    def test_zrange_empty_key(self, ctx):
        cmd = ZRangeCommand(['zrange', 'myset', '0', '-1'])
        result = cmd.execute(ctx)
        assert result == []

    def test_zrange_all_members(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRangeCommand(['zrange', 'myset', '0', '-1'])
        result = cmd.execute(ctx)
        assert result == ['member1', 'member2', 'member3']

    def test_zrange_with_scores(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRangeCommand(['zrange', 'myset', '0', '-1', 'WITHSCORES'])
        result = cmd.execute(ctx)
        assert result == ['member1', 1.0, 'member2', 2.0]

    def test_zrange_with_limit(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRangeCommand(['zrange', 'myset', '1', '2'])
        result = cmd.execute(ctx)
        assert result == ['member2', 'member3']

    def test_zrange_reverse(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRangeCommand(['zrange', 'myset', '0', '-1', 'REV'])
        result = cmd.execute(ctx)
        assert result == ['member3', 'member2', 'member1']

    def test_zrange_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZRangeCommand(['zrange', 'myset', '0', '-1'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)


class TestZRangeByScoreCommand:
    def test_zrangebyscore_empty_key(self, ctx):
        cmd = ZRangeByScoreCommand(['zrangebyscore', 'myset', '-inf', '+inf'])
        result = cmd.execute(ctx)
        assert result == []

    def test_zrangebyscore_with_members(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRangeByScoreCommand(['zrangebyscore', 'myset', '1.5', '3.0'])
        result = cmd.execute(ctx)
        assert result == ['member2', 'member3']

    def test_zrangebyscore_with_scores(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRangeByScoreCommand(['zrangebyscore', 'myset', '0', '3', 'WITHSCORES'])
        result = cmd.execute(ctx)
        assert result == ['member1', 1.0, 'member2', 2.0]

    def test_zrangebyscore_with_limit(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRangeByScoreCommand(['zrangebyscore', 'myset', '0', '3', 'LIMIT', '1', '1'])
        result = cmd.execute(ctx)
        assert result == ['member2']

    def test_zrangebyscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZRangeByScoreCommand(['zrangebyscore', 'myset', '0', '1'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zrangebyscore_invalid_score(self, ctx):
        with pytest.raises(ValueError, match='min and max must be valid float numbers'):
            ZRangeByScoreCommand(['zrangebyscore', 'myset', 'notanumber', '1'])


class TestZRevRangeByScoreCommand:
    def test_zrevrangebyscore_empty_key(self, ctx):
        cmd = ZRevRangeByScoreCommand(['zrevrangebyscore', 'myset', '+inf', '-inf'])
        result = cmd.execute(ctx)
        assert result == []

    def test_zrevrangebyscore_with_members(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRevRangeByScoreCommand(['zrevrangebyscore', 'myset', '3.0', '1.5'])
        result = cmd.execute(ctx)
        assert result == ['member3', 'member2']

    def test_zrevrangebyscore_with_scores(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRevRangeByScoreCommand(['zrevrangebyscore', 'myset', '3', '0', 'WITHSCORES'])
        result = cmd.execute(ctx)
        assert result == ['member2', 2.0, 'member1', 1.0]

    def test_zrevrangebyscore_with_limit(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRevRangeByScoreCommand(['zrevrangebyscore', 'myset', '3', '0', 'LIMIT', '1', '1'])
        result = cmd.execute(ctx)
        assert result == ['member2']

    def test_zrevrangebyscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZRevRangeByScoreCommand(['zrevrangebyscore', 'myset', '1', '0'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zrevrangebyscore_invalid_score(self, ctx):
        with pytest.raises(ValueError, match='min and max must be valid float numbers'):
            ZRevRangeByScoreCommand(['zrevrangebyscore', 'myset', 'notanumber', '1'])


class TestZRankCommand:
    def test_zrank_empty_key(self, ctx):
        cmd = ZRankCommand(['zrank', 'myset', 'member1'])
        result = cmd.execute(ctx)
        assert result is None

    def test_zrank_nonexistent_member(self, ctx):
        # Add some members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRankCommand(['zrank', 'myset', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result is None

    def test_zrank_with_members(self, ctx):
        # Add members with different scores
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRankCommand(['zrank', 'myset', 'member2'])
        result = cmd.execute(ctx)
        assert result == 1  # member2 is at index 1 (scores ordered ascending)

    def test_zrank_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZRankCommand(['zrank', 'myset', 'member1'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zrank_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zrank command requires key and member'):
            ZRankCommand(['zrank'])

    def test_zrank_with_scores(self, ctx):
        # Add members with different scores
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRankCommand(['zrank', 'myset', 'member2', 'WITHSCORES'])
        result = cmd.execute(ctx)
        assert result == [1, 2.0]  # rank=1, score=2.0

    def test_zrank_nonexistent_member_with_scores(self, ctx):
        # Add some members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRankCommand(['zrank', 'myset', 'nonexistent', 'WITHSCORES'])
        result = cmd.execute(ctx)
        assert result is None


class TestZRemCommand:
    def test_zrem_empty_key(self, ctx):
        cmd = ZRemCommand(['zrem', 'myset', 'member1'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_zrem_single_member(self, ctx):
        # Add members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRemCommand(['zrem', 'myset', 'member1'])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('myset').score('member1') is None
        assert ctx.db.get('myset').score('member2') == 2.0

    def test_zrem_multiple_members(self, ctx):
        # Add members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRemCommand(['zrem', 'myset', 'member1', 'member2', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == 2  # Only member1 and member2 were removed

        zset = ctx.db.get('myset')
        assert len(zset) == 1
        assert zset.score('member3') == 3.0

    def test_zrem_all_members(self, ctx):
        # Add members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRemCommand(['zrem', 'myset', 'member1', 'member2'])
        result = cmd.execute(ctx)
        assert result == 2
        assert not ctx.db.exists('myset')  # Key should be removed when empty

    def test_zrem_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZRemCommand(['zrem', 'myset', 'member1'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zrem_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zrem command requires key and at least one member'):
            ZRemCommand(['zrem', 'myset'])


class TestZRemRangeByScoreCommand:
    def test_zremrangebyscore_empty_key(self, ctx):
        cmd = ZRemRangeByScoreCommand(['zremrangebyscore', 'myset', '0', '10'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_zremrangebyscore_with_members(self, ctx):
        # Add members with different scores
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2',
                            '3.0', 'member3', '4.0', 'member4'])
        zadd.execute(ctx)

        cmd = ZRemRangeByScoreCommand(['zremrangebyscore', 'myset', '2', '3'])
        result = cmd.execute(ctx)
        assert result == 2  # member2 and member3 removed

        zset = ctx.db.get('myset')
        assert len(zset) == 2
        assert zset.score('member1') == 1.0
        assert zset.score('member4') == 4.0

    def test_zremrangebyscore_all_members(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRemRangeByScoreCommand(['zremrangebyscore', 'myset', '0', '3'])
        result = cmd.execute(ctx)
        assert result == 2
        assert not ctx.db.exists('myset')  # Key should be removed when empty

    def test_zremrangebyscore_invalid_range(self, ctx):
        with pytest.raises(ValueError, match='min and max must be valid float numbers'):
            ZRemRangeByScoreCommand(['zremrangebyscore', 'myset', 'notanumber', '10'])

    def test_zremrangebyscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZRemRangeByScoreCommand(['zremrangebyscore', 'myset', '0', '10'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)


class TestZRevRankCommand:
    def test_zrevrank_empty_key(self, ctx):
        cmd = ZRevRankCommand(['zrevrank', 'myset', 'member1'])
        result = cmd.execute(ctx)
        assert result is None

    def test_zrevrank_nonexistent_member(self, ctx):
        # Add some members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRevRankCommand(['zrevrank', 'myset', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result is None

    def test_zrevrank_with_members(self, ctx):
        # Add members with different scores
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRevRankCommand(['zrevrank', 'myset', 'member2'])
        result = cmd.execute(ctx)
        assert result == 1  # member2 is at index 1 from highest score

    def test_zrevrank_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZRevRankCommand(['zrevrank', 'myset', 'member1'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zrevrank_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zrevrank command requires key and member'):
            ZRevRankCommand(['zrevrank'])

    def test_zrevrank_with_scores(self, ctx):
        # Add members with different scores
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZRevRankCommand(['zrevrank', 'myset', 'member2', 'WITHSCORES'])
        result = cmd.execute(ctx)
        assert result == [1, 2.0]  # rank=1 (from highest), score=2.0

    def test_zrevrank_nonexistent_member_with_scores(self, ctx):
        # Add some members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZRevRankCommand(['zrevrank', 'myset', 'nonexistent', 'WITHSCORES'])
        result = cmd.execute(ctx)
        assert result is None


class TestZScanCommand:
    def test_zscan_empty_key(self, ctx):
        cmd = ZScanCommand(['zscan', 'myset', '0'])
        result = cmd.execute(ctx)
        assert result == [0, []]

    def test_zscan_basic_scan(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZScanCommand(['zscan', 'myset', '0'])
        result = cmd.execute(ctx)
        assert result[0] == 0  # Cursor
        assert len(result[1]) == 6  # 3 members * 2 (member and score)

        # Verify all members and scores are present
        members_scores = result[1]
        assert 'member1' in members_scores
        assert 'member2' in members_scores
        assert 'member3' in members_scores

    def test_zscan_with_pattern(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset',
                            '1.0', 'key1',
                            '2.0', 'key2',
                            '3.0', 'other3'])
        zadd.execute(ctx)

        cmd = ZScanCommand(['zscan', 'myset', '0', 'MATCH', 'key*'])
        result = cmd.execute(ctx)

        members_scores = result[1]
        assert len(members_scores) == 4  # 2 matching members * 2 (member and score)
        assert 'key1' in members_scores
        assert 'key2' in members_scores
        assert 'other3' not in members_scores

    def test_zscan_with_count(self, ctx):
        # Add members
        zadd = ZAddCommand(['zadd', 'myset',
                            '1.0', 'member1',
                            '2.0', 'member2',
                            '3.0', 'member3'])
        zadd.execute(ctx)

        cmd = ZScanCommand(['zscan', 'myset', '0', 'COUNT', '2'])
        result = cmd.execute(ctx)

        assert len(result[1]) == 4  # 2 members * 2 (member and score)

    def test_zscan_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZScanCommand(['zscan', 'myset', '0'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zscan_invalid_cursor(self, ctx):
        with pytest.raises(ValueError, match='cursor must be non-negative'):
            ZScanCommand(['zscan', 'myset', '-1'])

    def test_zscan_invalid_count(self, ctx):
        with pytest.raises(ValueError, match='count must be a positive integer'):
            ZScanCommand(['zscan', 'myset', '0', 'COUNT', '0'])


class TestZScoreCommand:
    def test_zscore_empty_key(self, ctx):
        cmd = ZScoreCommand(['zscore', 'myset', 'member1'])
        result = cmd.execute(ctx)
        assert result is None

    def test_zscore_nonexistent_member(self, ctx):
        # Add some members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1'])
        zadd.execute(ctx)

        cmd = ZScoreCommand(['zscore', 'myset', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result is None

    def test_zscore_existing_member(self, ctx):
        # Add member with score
        zadd = ZAddCommand(['zadd', 'myset', '1.5', 'member1'])
        zadd.execute(ctx)

        cmd = ZScoreCommand(['zscore', 'myset', 'member1'])
        result = cmd.execute(ctx)
        assert result == 1.5

    def test_zscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZScoreCommand(['zscore', 'myset', 'member1'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zscore_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zscore command requires key and member'):
            ZScoreCommand(['zscore'])


class TestZUnionCommand:
    def test_zunion_empty_keys(self, ctx):
        cmd = ZUnionCommand(['zunion', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert result == []

    def test_zunion_single_nonempty_set(self, ctx):
        # Add members to first set
        zadd = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2'])
        zadd.execute(ctx)

        cmd = ZUnionCommand(['zunion', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert result == ['member1', 'member2']

    def test_zunion_multiple_sets(self, ctx):
        # Add members to first set
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2'])
        zadd1.execute(ctx)

        # Add members to second set
        zadd2 = ZAddCommand(['zadd', 'set2', '2.0', 'member2', '3.0', 'member3'])
        zadd2.execute(ctx)

        cmd = ZUnionCommand(['zunion', '2', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert set(result) == {'member1', 'member2', 'member3'}

    def test_zunion_with_scores(self, ctx):
        # Add members to sets
        zadd1 = ZAddCommand(['zadd', 'set1', '1.0', 'member1', '2.0', 'member2'])
        zadd1.execute(ctx)
        zadd2 = ZAddCommand(['zadd', 'set2', '3.0', 'member2', '4.0', 'member3'])
        zadd2.execute(ctx)

        cmd = ZUnionCommand(['zunion', '2', 'set1', 'set2', 'WITHSCORES'])
        result = cmd.execute(ctx)

        # Convert result to dict for easier comparison
        scores = {result[i]: result[i + 1] for i in range(0, len(result), 2)}
        assert scores['member1'] == 1.0
        assert scores['member2'] == 5.0  # Takes the highest score
        assert scores['member3'] == 4.0

    def test_zunion_wrong_type(self, ctx):
        ctx.db.set('set1', "string")  # Wrong type
        cmd = ZUnionCommand(['zunion', '2', 'set1', 'set2'])
        with pytest.raises(TypeError, match="value at set1 is not a sorted set"):
            cmd.execute(ctx)

    def test_zunion_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zunion command requires numkeys and at least one key'):
            ZUnionCommand(['zunion'])

        with pytest.raises(ValueError, match='numkeys must be positive'):
            ZUnionCommand(['zunion', '0', 'set1'])


class TestZMScoreCommand:
    def test_zmscore_empty_key(self, ctx):
        cmd = ZMScoreCommand(['zmscore', 'myset', 'member1', 'member2'])
        result = cmd.execute(ctx)
        assert result == [None, None]

    def test_zmscore_single_member(self, ctx):
        # Add member with score
        zadd = ZAddCommand(['zadd', 'myset', '1.5', 'member1'])
        zadd.execute(ctx)

        cmd = ZMScoreCommand(['zmscore', 'myset', 'member1'])
        result = cmd.execute(ctx)
        assert result == [1.5]

    def test_zmscore_multiple_members(self, ctx):
        # Add members with scores
        zadd = ZAddCommand(['zadd', 'myset', '1.5', 'member1', '2.5', 'member2'])
        zadd.execute(ctx)

        cmd = ZMScoreCommand(['zmscore', 'myset', 'member1', 'member2', 'nonexistent'])
        result = cmd.execute(ctx)
        assert result == [1.5, 2.5, None]

    def test_zmscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        cmd = ZMScoreCommand(['zmscore', 'myset', 'member1'])
        with pytest.raises(TypeError, match="value is not a sorted set"):
            cmd.execute(ctx)

    def test_zmscore_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zmscore command requires key and at least one member'):
            ZMScoreCommand(['zmscore'])

    def test_zmscore_all_nonexistent(self, ctx):
        # Add some members first
        zadd = ZAddCommand(['zadd', 'myset', '1.0', 'member1'])
        zadd.execute(ctx)

        cmd = ZMScoreCommand(['zmscore', 'myset', 'nonexistent1', 'nonexistent2'])
        result = cmd.execute(ctx)
        assert result == [None, None]
