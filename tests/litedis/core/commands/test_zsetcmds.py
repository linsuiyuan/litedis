import pytest

from litedis.core.command.base import CommandContext
from litedis.core.command.sortedset import SortedSet
from litedis.core.command.zsetcmds import (
    ZAddCommand,
    ZCardCommand,
    ZCountCommand,
    ZDiffCommand,
    ZIncrByCommand,
    ZInterCommand,
    ZInterCardCommand,
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
from litedis.core.persistence.ldb import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db, [])


class TestZAddCommand:
    def test_zadd_new_key(self, ctx):
        ctx.cmdtokens = ['zadd', 'myset', '1.5', 'member1', '2.0', 'member2']
        cmd = ZAddCommand()
        result = cmd.execute(ctx)

        assert result == 2
        assert isinstance(ctx.db.get('myset'), SortedSet)
        assert ctx.db.get('myset').score('member1') == 1.5
        assert ctx.db.get('myset').score('member2') == 2.0

    def test_zadd_existing_members(self, ctx):
        # First add
        ctx.cmdtokens = ['zadd', 'myset', '1.5', 'member1']
        cmd1 = ZAddCommand()
        cmd1.execute(ctx)

        # Update existing member
        ctx.cmdtokens = ['zadd', 'myset', '2.0', 'member1', '3.0', 'member2']
        cmd2 = ZAddCommand()
        result = cmd2.execute(ctx)

        assert result == 1  # Only member2 is new
        assert ctx.db.get('myset').score('member1') == 2.0
        assert ctx.db.get('myset').score('member2') == 3.0

    def test_zadd_invalid_score(self, ctx):
        with pytest.raises(ValueError, match='invalid score'):
            ctx.cmdtokens = ['zadd', 'myset', 'notanumber', 'member1']
            ZAddCommand().execute(ctx)

    def test_zadd_wrong_args(self, ctx):
        with pytest.raises(ValueError, match='zadd command requires key, score and member'):
            ctx.cmdtokens = ['zadd', 'myset']
            ZAddCommand().execute(ctx)

        with pytest.raises(ValueError, match='score and member must come in pairs'):
            ctx.cmdtokens = ['zadd', 'myset', '1.5', 'member1', '2.0']
            ZAddCommand().execute(ctx)


class TestZCardCommand:
    def test_zcard_empty_key(self, ctx):
        ctx.cmdtokens = ['zcard', 'myset']
        cmd = ZCardCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_zcard_with_members(self, ctx):
        # Add some members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zcard', 'myset']
        cmd = ZCardCommand()
        result = cmd.execute(ctx)
        assert result == 3

    def test_zcard_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zcard', 'myset']
        cmd = ZCardCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zcard_wrong_args(self, ctx):
        ctx.cmdtokens = ['zcard']
        with pytest.raises(ValueError, match='zcard command requires key'):
            ZCardCommand().execute(ctx)


class TestZCountCommand:
    def test_zcount_empty_key(self, ctx):
        ctx.cmdtokens = ['zcount', 'myset', '0', '10']
        cmd = ZCountCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_zcount_with_members(self, ctx):
        # Add members with different scores
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zcount', 'myset', '1.5', '3.0']
        cmd = ZCountCommand()
        result = cmd.execute(ctx)
        assert result == 2  # member2 and member3

    def test_zcount_invalid_range(self, ctx):
        ctx.cmdtokens = ['zcount', 'myset', 'notanumber', '10']
        cmd = ZCountCommand()
        with pytest.raises(ValueError, match='min and max must be valid float numbers'):
            cmd.execute(ctx)

    def test_zcount_wrong_args(self, ctx):
        ctx.cmdtokens = ['zcount', 'myset']
        with pytest.raises(ValueError, match='zcount command requires key, min and max'):
            ZCountCommand().execute(ctx)


class TestZDiffCommand:
    def test_zdiff_empty_keys(self, ctx):
        ctx.cmdtokens = ['zdiff', '2', 'set1', 'set2']
        cmd = ZDiffCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_zdiff_with_members(self, ctx):
        # Add members to first set
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)

        # Add members to second set
        ctx.cmdtokens = ['zadd', 'set2', '2.0', 'member2', '4.0', 'member4']
        zadd2 = ZAddCommand()
        zadd2.execute(ctx)

        ctx.cmdtokens = ['zdiff', '2', 'set1', 'set2']
        cmd = ZDiffCommand()
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert 'member1' in result
        assert 'member3' in result

    def test_zdiff_wrong_type(self, ctx):
        ctx.db.set('set1', "string")  # Wrong type
        ctx.cmdtokens = ['zdiff', '2', 'set1', 'set2']
        cmd = ZDiffCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zdiff_wrong_args(self, ctx):
        ctx.cmdtokens = ['zdiff']
        with pytest.raises(ValueError, match='zdiff command requires numkeys and at least one key'):
            ZDiffCommand().execute(ctx)

        with pytest.raises(ValueError, match='numkeys must be positive'):
            ctx.cmdtokens = ['zdiff', '-1', 'set1']
            ZDiffCommand().execute(ctx)

    def test_zdiff_with_scores(self, ctx):
        # Add members to first set
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)

        # Add members to second set
        ctx.cmdtokens = ['zadd', 'set2', '2.0', 'member2', '4.0', 'member4']
        zadd2 = ZAddCommand()
        zadd2.execute(ctx)

        ctx.cmdtokens = ['zdiff', '2', 'set1', 'set2', 'WITHSCORES']
        cmd = ZDiffCommand()
        result = cmd.execute(ctx)

        # Convert result to dict for easier comparison
        scores = {result[i]: result[i + 1] for i in range(0, len(result), 2)}
        assert len(scores) == 2
        assert scores['member1'] == 1.0
        assert scores['member3'] == 3.0


class TestZIncrByCommand:
    def test_zincrby_new_member(self, ctx):
        ctx.cmdtokens = ['zincrby', 'myset', '1.5', 'member1']
        cmd = ZIncrByCommand()
        result = cmd.execute(ctx)
        assert result == 1.5
        assert ctx.db.get('myset').score('member1') == 1.5

    def test_zincrby_existing_member(self, ctx):
        # First add member
        ctx.cmdtokens = ['zadd', 'myset', '2.0', 'member1']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        # Increment score
        ctx.cmdtokens = ['zincrby', 'myset', '1.5', 'member1']
        cmd = ZIncrByCommand()
        result = cmd.execute(ctx)
        assert result == 3.5
        assert ctx.db.get('myset').score('member1') == 3.5

    def test_zincrby_invalid_increment(self, ctx):
        ctx.cmdtokens = ['zincrby', 'myset', 'notanumber', 'member1']
        cmd = ZIncrByCommand()
        with pytest.raises(ValueError, match='increment must be a valid float number'):
            cmd.execute(ctx)

    def test_zincrby_wrong_args(self, ctx):
        ctx.cmdtokens = ['zincrby', 'myset']
        with pytest.raises(ValueError, match='zincrby command requires key, increment and member'):
            ZIncrByCommand().execute(ctx)


class TestZInterCommand:
    def test_zinter_empty_keys(self, ctx):
        ctx.cmdtokens = ['zinter', '2', 'set1', 'set2']
        cmd = ZInterCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_zinter_with_members(self, ctx):
        # Add members to first set
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)

        # Add members to second set
        ctx.cmdtokens = ['zadd', 'set2', '2.0', 'member2', '3.0', 'member3', '4.0', 'member4']
        zadd2 = ZAddCommand()
        zadd2.execute(ctx)

        ctx.cmdtokens = ['zinter', '2', 'set1', 'set2']
        cmd = ZInterCommand()
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert 'member2' in result
        assert 'member3' in result

    def test_zinter_with_withscores(self, ctx):
        # Add members to sets
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1', '2.0', 'member2']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)

        ctx.cmdtokens = ['zadd', 'set2', '2.0', 'member2', '3.0', 'member3']
        zadd2 = ZAddCommand()
        zadd2.execute(ctx)

        ctx.cmdtokens = ['zinter', '2', 'set1', 'set2', 'WITHSCORES']
        cmd = ZInterCommand()
        result = cmd.execute(ctx)
        assert result == ['member2', 2.0]

    def test_zinter_wrong_type(self, ctx):
        ctx.db.set('set1', "string")  # Wrong type
        ctx.cmdtokens = ['zinter', '2', 'set1', 'set2']
        cmd = ZInterCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zinter_wrong_args(self, ctx):
        ctx.cmdtokens = ['zinter']
        with pytest.raises(ValueError, match='zinter command requires numkeys and at least one key'):
            ZInterCommand().execute(ctx)

        ctx.cmdtokens = ['zinter', '-1', 'set1']
        with pytest.raises(ValueError, match='numkeys must be positive'):
            ZInterCommand().execute(ctx)


class TestZInterCardCommand:
    def test_zintercard_empty_keys(self, ctx):
        ctx.cmdtokens = ['zintercard', '2', 'set1', 'set2']
        cmd = ZInterCardCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_zintercard_with_members(self, ctx):
        # Add members to sets
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)

        ctx.cmdtokens = ['zadd', 'set2', '2.0', 'member2', '3.0', 'member3', '4.0', 'member4']
        zadd2 = ZAddCommand()
        zadd2.execute(ctx)

        ctx.cmdtokens = ['zintercard', '2', 'set1', 'set2']
        cmd = ZInterCardCommand()
        result = cmd.execute(ctx)
        assert result == 2  # member2 and member3

    def test_zintercard_with_limit(self, ctx):
        # Add members to sets
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)

        ctx.cmdtokens = ['zadd', 'set2', '2.0', 'member2', '3.0', 'member3', '4.0', 'member4']
        zadd2 = ZAddCommand()
        zadd2.execute(ctx)

        ctx.cmdtokens = ['zintercard', '2', 'set1', 'set2', 'LIMIT', '1']
        cmd = ZInterCardCommand()
        result = cmd.execute(ctx)
        assert result == 1

    def test_zintercard_wrong_type(self, ctx):
        ctx.db.set('set1', "string")  # Wrong type
        ctx.cmdtokens = ['zintercard', '2', 'set1', 'set2']
        cmd = ZInterCardCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zintercard_invalid_numkeys(self, ctx):
        ctx.cmdtokens = ['zintercard', '-1', 'set1']
        cmd = ZInterCardCommand()
        with pytest.raises(ValueError, match="numkeys must be positive"):
            cmd.execute(ctx)

    def test_zintercard_invalid_limit(self, ctx):
        ctx.cmdtokens = ['zintercard', '1', 'set1', 'LIMIT', '-1']
        cmd = ZInterCardCommand()
        with pytest.raises(ValueError, match="limit must be non-negative"):
            cmd.execute(ctx)


class TestZPopMaxCommand:
    def test_zpopmax_empty_key(self, ctx):
        ctx.cmdtokens = ['zpopmax', 'myset']
        cmd = ZPopMaxCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_zpopmax_single_member(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zpopmax', 'myset']
        cmd = ZPopMaxCommand()
        result = cmd.execute(ctx)
        assert len(result) == 1
        assert result[0] == ('member3', 3.0)

        # Verify member was removed
        assert ctx.db.get('myset').score('member3') is None

    def test_zpopmax_multiple_members(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zpopmax', 'myset', '2']
        cmd = ZPopMaxCommand()
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
        ctx.cmdtokens = ['zpopmax', 'myset']
        cmd = ZPopMaxCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)


class TestZPopMinCommand:
    def test_zpopmin_empty_key(self, ctx):
        ctx.cmdtokens = ['zpopmin', 'myset']
        cmd = ZPopMinCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_zpopmin_single_member(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zpopmin', 'myset']
        cmd = ZPopMinCommand()
        result = cmd.execute(ctx)
        assert len(result) == 1
        assert result[0] == ('member1', 1.0)

        # Verify member was removed
        assert ctx.db.get('myset').score('member1') is None

    def test_zpopmin_multiple_members(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zpopmin', 'myset', '2']
        cmd = ZPopMinCommand()
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
        ctx.cmdtokens = ['zpopmin', 'myset']
        cmd = ZPopMinCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)


class TestZRandMemberCommand:
    def test_zrandmember_empty_key(self, ctx):
        ctx.cmdtokens = ['zrandmember', 'myset']
        cmd = ZRandMemberCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_zrandmember_single_member(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrandmember', 'myset']
        cmd = ZRandMemberCommand()
        result = cmd.execute(ctx)
        assert isinstance(result, str)
        assert result in ['member1', 'member2', 'member3']

    def test_zrandmember_multiple_members(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrandmember', 'myset', '2']
        cmd = ZRandMemberCommand()
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert all(member in ['member1', 'member2', 'member3'] for member in result)

    def test_zrandmember_with_scores(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrandmember', 'myset', '1', 'WITHSCORES']
        cmd = ZRandMemberCommand()
        result = cmd.execute(ctx)
        assert len(result) == 2  # [member, score]
        assert result[0] in ['member1', 'member2']
        assert result[1] in [1.0, 2.0]

    def test_zrandmember_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zrandmember', 'myset']
        cmd = ZRandMemberCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zrandmember_invalid_count(self, ctx):
        ctx.cmdtokens = ['zrandmember', 'myset', 'notanumber']
        cmd = ZRandMemberCommand()
        with pytest.raises(ValueError, match='count must be an integer'):
            cmd.execute(ctx)


class TestZMPopCommand:
    def test_zmpop_empty_key(self, ctx):
        ctx.cmdtokens = ['zmpop', '1', 'myset', 'MIN']
        cmd = ZMPopCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_zmpop_single_key_min(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zmpop', '1', 'myset', 'MIN']
        cmd = ZMPopCommand()
        result = cmd.execute(ctx)
        assert result == ['myset', [('member1', 1.0)]]

    def test_zmpop_single_key_max(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zmpop', '1', 'myset', 'MAX']
        cmd = ZMPopCommand()
        result = cmd.execute(ctx)
        assert result == ['myset', [('member2', 2.0)]]

    def test_zmpop_multiple_keys(self, ctx):
        # Add members to different sets
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)
        ctx.cmdtokens = ['zadd', 'set2', '2.0', 'member2']
        zadd2 = ZAddCommand()
        zadd2.execute(ctx)

        ctx.cmdtokens = ['zmpop', '2', 'set1', 'set2', 'MIN']
        cmd = ZMPopCommand()
        result = cmd.execute(ctx)
        assert result == ['set1', [('member1', 1.0)]]

    def test_zmpop_with_count(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zmpop', '1', 'myset', 'MIN', 'COUNT', '2']
        cmd = ZMPopCommand()
        result = cmd.execute(ctx)
        assert result == ['myset', [('member1', 1.0), ('member2', 2.0)]]

    def test_zmpop_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zmpop', '1', 'myset', 'MIN']
        cmd = ZMPopCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zmpop_invalid_args(self, ctx):
        ctx.cmdtokens = ['zmpop', '1', 'myset', 'INVALID']
        cmd = ZMPopCommand()
        with pytest.raises(ValueError, match='WHERE must be either MIN or MAX'):
            cmd.execute(ctx)


class TestZRangeCommand:
    def test_zrange_empty_key(self, ctx):
        ctx.cmdtokens = ['zrange', 'myset', '0', '-1']
        cmd = ZRangeCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_zrange_all_members(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrange', 'myset', '0', '-1']
        cmd = ZRangeCommand()
        result = cmd.execute(ctx)
        assert result == ['member1', 'member2', 'member3']

    def test_zrange_with_scores(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrange', 'myset', '0', '-1', 'WITHSCORES']
        cmd = ZRangeCommand()
        result = cmd.execute(ctx)
        assert result == ['member1', 1.0, 'member2', 2.0]

    def test_zrange_with_limit(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrange', 'myset', '1', '2']
        cmd = ZRangeCommand()
        result = cmd.execute(ctx)
        assert result == ['member2', 'member3']

    def test_zrange_reverse(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrange', 'myset', '0', '-1', 'REV']
        cmd = ZRangeCommand()
        result = cmd.execute(ctx)
        assert result == ['member3', 'member2', 'member1']

    def test_zrange_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zrange', 'myset', '0', '-1']
        cmd = ZRangeCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)


class TestZRangeByScoreCommand:
    def test_zrangebyscore_empty_key(self, ctx):
        ctx.cmdtokens = ['zrangebyscore', 'myset', '-inf', '+inf']
        cmd = ZRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_zrangebyscore_with_members(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrangebyscore', 'myset', '1.5', '3.0']
        cmd = ZRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == ['member2', 'member3']

    def test_zrangebyscore_with_scores(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrangebyscore', 'myset', '0', '3', 'WITHSCORES']
        cmd = ZRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == ['member1', 1.0, 'member2', 2.0]

    def test_zrangebyscore_with_limit(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrangebyscore', 'myset', '0', '3', 'LIMIT', '1', '1']
        cmd = ZRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == ['member2']

    def test_zrangebyscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zrangebyscore', 'myset', '0', '1']
        cmd = ZRangeByScoreCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zrangebyscore_invalid_score(self, ctx):
        ctx.cmdtokens = ['zrangebyscore', 'myset', 'notanumber', '1']
        cmd = ZRangeByScoreCommand()
        with pytest.raises(ValueError, match='min and max must be valid float numbers'):
            cmd.execute(ctx)


class TestZRevRangeByScoreCommand:
    def test_zrevrangebyscore_empty_key(self, ctx):
        ctx.cmdtokens = ['zrevrangebyscore', 'myset', '+inf', '-inf']
        cmd = ZRevRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_zrevrangebyscore_with_members(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrevrangebyscore', 'myset', '3.0', '1.5']
        cmd = ZRevRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == ['member3', 'member2']

    def test_zrevrangebyscore_with_scores(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrevrangebyscore', 'myset', '3', '0', 'WITHSCORES']
        cmd = ZRevRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == ['member2', 2.0, 'member1', 1.0]

    def test_zrevrangebyscore_with_limit(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd = ZAddCommand()
        zadd.execute(ctx)

        ctx.cmdtokens = ['zrevrangebyscore', 'myset', '3', '0', 'LIMIT', '1', '1']
        cmd = ZRevRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == ['member2']

    def test_zrevrangebyscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zrevrangebyscore', 'myset', '1', '0']
        cmd = ZRevRangeByScoreCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zrevrangebyscore_invalid_score(self, ctx):
        ctx.cmdtokens = ['zrevrangebyscore', 'myset', 'notanumber', '1']
        cmd = ZRevRangeByScoreCommand()
        with pytest.raises(ValueError, match='min and max must be valid float numbers'):
            cmd.execute(ctx)


class TestZRankCommand:
    def test_zrank_empty_key(self, ctx):
        ctx.cmdtokens = ['zrank', 'myset', 'member1']
        cmd = ZRankCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_zrank_nonexistent_member(self, ctx):
        # Add some members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrank', 'myset', 'nonexistent']
        cmd = ZRankCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_zrank_with_members(self, ctx):
        # Add members with different scores
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrank', 'myset', 'member2']
        cmd = ZRankCommand()
        result = cmd.execute(ctx)
        assert result == 1  # member2 is at index 1 (scores ordered ascending)

    def test_zrank_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zrank', 'myset', 'member1']
        cmd = ZRankCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zrank_wrong_args(self, ctx):
        ctx.cmdtokens = ['zrank']
        with pytest.raises(ValueError, match='zrank command requires key and member'):
            ZRankCommand().execute(ctx)

    def test_zrank_with_scores(self, ctx):
        # Add members with different scores
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrank', 'myset', 'member2', 'WITHSCORES']
        cmd = ZRankCommand()
        result = cmd.execute(ctx)
        assert result == [1, 2.0]  # rank=1, score=2.0

    def test_zrank_nonexistent_member_with_scores(self, ctx):
        # Add some members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrank', 'myset', 'nonexistent', 'WITHSCORES']
        cmd = ZRankCommand()
        result = cmd.execute(ctx)
        assert result is None


class TestZRemCommand:
    def test_zrem_empty_key(self, ctx):
        ctx.cmdtokens = ['zrem', 'myset', 'member1']
        cmd = ZRemCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_zrem_single_member(self, ctx):
        # Add members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrem', 'myset', 'member1']
        cmd = ZRemCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('myset').score('member1') is None
        assert ctx.db.get('myset').score('member2') == 2.0

    def test_zrem_multiple_members(self, ctx):
        # Add members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrem', 'myset', 'member1', 'member2', 'nonexistent']
        cmd = ZRemCommand()
        result = cmd.execute(ctx)
        assert result == 2  # Only member1 and member2 were removed

        zset = ctx.db.get('myset')
        assert len(zset) == 1
        assert zset.score('member3') == 3.0

    def test_zrem_all_members(self, ctx):
        # Add members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrem', 'myset', 'member1', 'member2']
        cmd = ZRemCommand()
        result = cmd.execute(ctx)
        assert result == 2
        assert not ctx.db.exists('myset')  # Key should be removed when empty

    def test_zrem_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zrem', 'myset', 'member1']
        cmd = ZRemCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zrem_wrong_args(self, ctx):
        ctx.cmdtokens = ['zrem', 'myset']
        with pytest.raises(ValueError, match='zrem command requires key and at least one member'):
            ZRemCommand().execute(ctx)


class TestZRemRangeByScoreCommand:
    def test_zremrangebyscore_empty_key(self, ctx):
        ctx.cmdtokens = ['zremrangebyscore', 'myset', '0', '10']
        cmd = ZRemRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_zremrangebyscore_with_members(self, ctx):
        # Add members with different scores
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3', '4.0', 'member4']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zremrangebyscore', 'myset', '2', '3']
        cmd = ZRemRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == 2  # member2 and member3 removed

        zset = ctx.db.get('myset')
        assert len(zset) == 2
        assert zset.score('member1') == 1.0
        assert zset.score('member4') == 4.0

    def test_zremrangebyscore_all_members(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zremrangebyscore', 'myset', '0', '3']
        cmd = ZRemRangeByScoreCommand()
        result = cmd.execute(ctx)
        assert result == 2
        assert not ctx.db.exists('myset')  # Key should be removed when empty

    def test_zremrangebyscore_invalid_range(self, ctx):
        ctx.cmdtokens = ['zremrangebyscore', 'myset', 'notanumber', '10']
        cmd = ZRemRangeByScoreCommand()
        with pytest.raises(ValueError, match='min and max must be valid float numbers'):
            cmd.execute(ctx)

    def test_zremrangebyscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zremrangebyscore', 'myset', '0', '10']
        cmd = ZRemRangeByScoreCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)


class TestZRevRankCommand:
    def test_zrevrank_empty_key(self, ctx):
        ctx.cmdtokens = ['zrevrank', 'myset', 'member1']
        cmd = ZRevRankCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_zrevrank_nonexistent_member(self, ctx):
        # Add some members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrevrank', 'myset', 'nonexistent']
        cmd = ZRevRankCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_zrevrank_with_members(self, ctx):
        # Add members with different scores
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrevrank', 'myset', 'member2']
        cmd = ZRevRankCommand()
        result = cmd.execute(ctx)
        assert result == 1  # member2 is at index 1 from highest score

    def test_zrevrank_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zrevrank', 'myset', 'member1']
        cmd = ZRevRankCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zrevrank_wrong_args(self, ctx):
        ctx.cmdtokens = ['zrevrank']
        with pytest.raises(ValueError, match='zrevrank command requires key and member'):
            ZRevRankCommand().execute(ctx)

    def test_zrevrank_with_scores(self, ctx):
        # Add members with different scores
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrevrank', 'myset', 'member2', 'WITHSCORES']
        cmd = ZRevRankCommand()
        result = cmd.execute(ctx)
        assert result == [1, 2.0]  # rank=1 (from highest), score=2.0

    def test_zrevrank_nonexistent_member_with_scores(self, ctx):
        # Add some members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zrevrank', 'myset', 'nonexistent', 'WITHSCORES']
        cmd = ZRevRankCommand()
        result = cmd.execute(ctx)
        assert result is None


class TestZScanCommand:
    def test_zscan_empty_key(self, ctx):
        ctx.cmdtokens = ['zscan', 'myset', '0']
        cmd = ZScanCommand()
        result = cmd.execute(ctx)
        assert result == [0, []]

    def test_zscan_basic_scan(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zscan', 'myset', '0']
        cmd = ZScanCommand()
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
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'key1', '2.0', 'key2', '3.0', 'other3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zscan', 'myset', '0', 'MATCH', 'key*']
        cmd = ZScanCommand()
        result = cmd.execute(ctx)

        members_scores = result[1]
        assert len(members_scores) == 4  # 2 matching members * 2 (member and score)
        assert 'key1' in members_scores
        assert 'key2' in members_scores
        assert 'other3' not in members_scores

    def test_zscan_with_count(self, ctx):
        # Add members
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zscan', 'myset', '0', 'COUNT', '2']
        cmd = ZScanCommand()
        result = cmd.execute(ctx)

        assert len(result[1]) == 4  # 2 members * 2 (member and score)

    def test_zscan_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zscan', 'myset', '0']
        cmd = ZScanCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zscan_invalid_cursor(self, ctx):
        ctx.cmdtokens = ['zscan', 'myset', '-1']
        cmd = ZScanCommand()
        with pytest.raises(ValueError, match='cursor must be non-negative'):
            cmd.execute(ctx)

    def test_zscan_invalid_count(self, ctx):
        ctx.cmdtokens = ['zscan', 'myset', '0', 'COUNT', '-1']
        cmd = ZScanCommand()
        with pytest.raises(ValueError, match='count must be a positive integer'):
            cmd.execute(ctx)


class TestZScoreCommand:
    def test_zscore_empty_key(self, ctx):
        ctx.cmdtokens = ['zscore', 'myset', 'member1']
        cmd = ZScoreCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_zscore_nonexistent_member(self, ctx):
        # Add some members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zscore', 'myset', 'nonexistent']
        cmd = ZScoreCommand()
        result = cmd.execute(ctx)
        assert result is None

    def test_zscore_existing_member(self, ctx):
        # Add member with score
        ctx.cmdtokens = ['zadd', 'myset', '1.5', 'member1']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zscore', 'myset', 'member1']
        cmd = ZScoreCommand()
        result = cmd.execute(ctx)
        assert result == 1.5

    def test_zscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zscore', 'myset', 'member1']
        cmd = ZScoreCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zscore_wrong_args(self, ctx):
        ctx.cmdtokens = ['zscore']
        with pytest.raises(ValueError, match='zscore command requires key and member'):
            ZScoreCommand().execute(ctx)


class TestZUnionCommand:
    def test_zunion_empty_keys(self, ctx):
        ctx.cmdtokens = ['zunion', '2', 'set1', 'set2']
        cmd = ZUnionCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_zunion_single_nonempty_set(self, ctx):
        # Add members to first set
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1', '2.0', 'member2']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)

        ctx.cmdtokens = ['zunion', '2', 'set1', 'set2']
        cmd = ZUnionCommand()
        result = cmd.execute(ctx)
        assert result == ['member1', 'member2']

    def test_zunion_multiple_sets(self, ctx):
        # Add members to first set
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1', '2.0', 'member2', '3.0', 'member3']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)

        # Add members to second set
        ctx.cmdtokens = ['zadd', 'set2', '2.0', 'member2', '4.0', 'member4']
        zadd2 = ZAddCommand()
        zadd2.execute(ctx)

        ctx.cmdtokens = ['zunion', '2', 'set1', 'set2']
        cmd = ZUnionCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'member1', 'member2', 'member3', 'member4'}

    def test_zunion_with_scores(self, ctx):
        # Add members to sets
        ctx.cmdtokens = ['zadd', 'set1', '1.0', 'member1', '2.0', 'member2']
        zadd1 = ZAddCommand()
        zadd1.execute(ctx)

        ctx.cmdtokens = ['zadd', 'set2', '3.0', 'member2', '4.0', 'member3']
        zadd2 = ZAddCommand()
        zadd2.execute(ctx)

        ctx.cmdtokens = ['zunion', '2', 'set1', 'set2', 'WITHSCORES']
        cmd = ZUnionCommand()
        result = cmd.execute(ctx)

        # Convert result to dict for easier comparison
        scores = {result[i]: result[i + 1] for i in range(0, len(result), 2)}
        assert scores['member1'] == 1.0
        assert scores['member2'] == 5.0  # Takes the highest score
        assert scores['member3'] == 4.0

    def test_zunion_wrong_type(self, ctx):
        ctx.db.set('set1', "string")  # Wrong type
        ctx.cmdtokens = ['zunion', '2', 'set1', 'set2']
        cmd = ZUnionCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zunion_wrong_args(self, ctx):
        ctx.cmdtokens = ['zunion']
        with pytest.raises(ValueError, match='zunion command requires numkeys and at least one key'):
            ZUnionCommand().execute(ctx)

        ctx.cmdtokens = ['zunion', '0', 'set1']
        with pytest.raises(ValueError, match='numkeys must be positive'):
            ZUnionCommand().execute(ctx)


class TestZMScoreCommand:
    def test_zmscore_empty_key(self, ctx):
        ctx.cmdtokens = ['zmscore', 'myset', 'member1', 'member2']
        cmd = ZMScoreCommand()
        result = cmd.execute(ctx)
        assert result == [None, None]

    def test_zmscore_single_member(self, ctx):
        # Add member with score
        ctx.cmdtokens = ['zadd', 'myset', '1.5', 'member1']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zmscore', 'myset', 'member1']
        cmd = ZMScoreCommand()
        result = cmd.execute(ctx)
        assert result == [1.5]

    def test_zmscore_multiple_members(self, ctx):
        # Add members with scores
        ctx.cmdtokens = ['zadd', 'myset', '1.5', 'member1', '2.5', 'member2']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zmscore', 'myset', 'member1', 'member2', 'nonexistent']
        cmd = ZMScoreCommand()
        result = cmd.execute(ctx)
        assert result == [1.5, 2.5, None]

    def test_zmscore_wrong_type(self, ctx):
        ctx.db.set('myset', "string")  # Wrong type
        ctx.cmdtokens = ['zmscore', 'myset', 'member1']
        cmd = ZMScoreCommand()
        with pytest.raises(TypeError, match="value is not a zset"):
            cmd.execute(ctx)

    def test_zmscore_wrong_args(self, ctx):
        ctx.cmdtokens = ['zmscore']
        with pytest.raises(ValueError, match='zmscore command requires key and at least one member'):
            ZMScoreCommand().execute(ctx)

    def test_zmscore_all_nonexistent(self, ctx):
        # Add some members first
        ctx.cmdtokens = ['zadd', 'myset', '1.0', 'member1']
        zadd_cmd = ZAddCommand()
        zadd_cmd.execute(ctx)

        ctx.cmdtokens = ['zmscore', 'myset', 'nonexistent1', 'nonexistent2']
        cmd = ZMScoreCommand()
        result = cmd.execute(ctx)
        assert result == [None, None]
