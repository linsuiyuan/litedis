import pytest

from litedis.core.command.base import CommandContext
from litedis.core.command.setcmds import (
    SAddCommand,
    SCardCommand,
    SDiffCommand,
    SInterCommand,
    SInterCardCommand,
    SIsMemberCommand,
    SMembersCommand,
    SMIsMemberCommand,
    SMoveCommand,
    SPopCommand,
    SRandMemberCommand,
    SRemCommand,
    SUnionCommand,
)
from litedis.core.persistence.ldb import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db, [])


class TestSAddCommand:
    def test_sadd_to_new_set(self, ctx):
        ctx.cmdtokens = ['sadd', 'myset', 'a', 'b']
        cmd = SAddCommand()
        assert cmd.execute(ctx) == 2
        assert ctx.db.get('myset') == {'a', 'b'}

    def test_sadd_to_existing_set(self, ctx):
        ctx.db.set('myset', {'a'})
        ctx.cmdtokens = ['sadd', 'myset', 'a', 'b']
        cmd = SAddCommand()
        assert cmd.execute(ctx) == 1
        assert ctx.db.get('myset') == {'a', 'b'}

    def test_sadd_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['sadd', 'myset', 'a']
        cmd = SAddCommand()
        assert cmd.execute(ctx) == 1
        assert ctx.db.get('myset') == {'a'}

    def test_sadd_invalid_type(self, ctx):
        ctx.db.set('mystr', 'not_a_set')
        ctx.cmdtokens = ['sadd', 'mystr', 'a']
        cmd = SAddCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)


class TestSCardCommand:
    def test_scard_empty_set(self, ctx):
        ctx.db.set('myset', set())
        ctx.cmdtokens = ['scard', 'myset']
        cmd = SCardCommand()
        assert cmd.execute(ctx) == 0

    def test_scard_populated_set(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['scard', 'myset']
        cmd = SCardCommand()
        assert cmd.execute(ctx) == 3

    def test_scard_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['scard', 'nosuchkey']
        cmd = SCardCommand()
        assert cmd.execute(ctx) == 0

    def test_scard_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        ctx.cmdtokens = ['scard', 'mystr']
        cmd = SCardCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_scard_invalid_syntax(self, ctx):
        with pytest.raises(ValueError, match="scard command requires key"):
            ctx.cmdtokens = ['scard']
            SCardCommand().execute(ctx)


class TestSDiffCommand:
    def test_sdiff_two_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        ctx.db.set('set2', {'b', 'c', 'd'})
        ctx.cmdtokens = ['sdiff', 'set1', 'set2']
        cmd = SDiffCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a'}

    def test_sdiff_multiple_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c', 'd'})
        ctx.db.set('set2', {'b'})
        ctx.db.set('set3', {'c'})
        ctx.cmdtokens = ['sdiff', 'set1', 'set2', 'set3']
        cmd = SDiffCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'd'}

    def test_sdiff_nonexistent_first_key(self, ctx):
        ctx.cmdtokens = ['sdiff', 'nosuchkey', 'set2']
        cmd = SDiffCommand()
        assert cmd.execute(ctx) == []

    def test_sdiff_nonexistent_other_key(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        ctx.cmdtokens = ['sdiff', 'set1', 'nosuchkey']
        cmd = SDiffCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b', 'c'}

    def test_sdiff_wrong_type(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('str1', 'string')
        ctx.cmdtokens = ['sdiff', 'set1', 'str1']
        cmd = SDiffCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_sdiff_invalid_syntax(self, ctx):
        with pytest.raises(ValueError, match="sdiff command requires at least one key"):
            ctx.cmdtokens = ['sdiff']
            SDiffCommand().execute(ctx)


class TestSInterCommand:
    def test_sinter_two_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        ctx.db.set('set2', {'b', 'c', 'd'})
        ctx.cmdtokens = ['sinter', 'set1', 'set2']
        cmd = SInterCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'b', 'c'}

    def test_sinter_multiple_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c', 'd'})
        ctx.db.set('set2', {'b', 'c'})
        ctx.db.set('set3', {'c', 'd'})
        ctx.cmdtokens = ['sinter', 'set1', 'set2', 'set3']
        cmd = SInterCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'c'}

    def test_sinter_empty_intersection(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('set2', {'c', 'd'})
        ctx.cmdtokens = ['sinter', 'set1', 'set2']
        cmd = SInterCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_sinter_nonexistent_key(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.cmdtokens = ['sinter', 'set1', 'nosuchkey']
        cmd = SInterCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_sinter_wrong_type(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('str1', 'string')
        ctx.cmdtokens = ['sinter', 'set1', 'str1']
        cmd = SInterCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_sinter_invalid_syntax(self, ctx):
        with pytest.raises(ValueError, match="sinter command requires at least one key"):
            ctx.cmdtokens = ['sinter']
            SInterCommand().execute(ctx)


class TestSInterCardCommand:
    def test_sintercard_two_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        ctx.db.set('set2', {'b', 'c', 'd'})
        ctx.cmdtokens = ['sintercard', '2', 'set1', 'set2']
        cmd = SInterCardCommand()
        assert cmd.execute(ctx) == 2  # {'b', 'c'}

    def test_sintercard_with_limit(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c', 'd'})
        ctx.db.set('set2', {'b', 'c', 'd'})
        ctx.cmdtokens = ['sintercard', '2', 'set1', 'set2', 'LIMIT', '1']
        cmd = SInterCardCommand()
        assert cmd.execute(ctx) == 1

    def test_sintercard_empty_intersection(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('set2', {'c', 'd'})
        ctx.cmdtokens = ['sintercard', '2', 'set1', 'set2']
        cmd = SInterCardCommand()
        assert cmd.execute(ctx) == 0

    def test_sintercard_nonexistent_key(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.cmdtokens = ['sintercard', '2', 'set1', 'nosuchkey']
        cmd = SInterCardCommand()
        assert cmd.execute(ctx) == 0

    def test_sintercard_wrong_type(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('str1', 'string')
        ctx.cmdtokens = ['sintercard', '2', 'set1', 'str1']
        cmd = SInterCardCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_sintercard_invalid_numkeys(self, ctx):
        ctx.cmdtokens = ['sintercard', '-1', 'set1']
        with pytest.raises(ValueError, match="numkeys must be positive"):
            SInterCardCommand().execute(ctx)

    def test_sintercard_invalid_limit(self, ctx):
        ctx.cmdtokens = ['sintercard', '1', 'set1', 'LIMIT', '-1']
        with pytest.raises(ValueError, match="limit must be non-negative"):
            SInterCardCommand().execute(ctx)


class TestSIsMemberCommand:
    def test_sismember_existing_member(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['sismember', 'myset', 'b']
        cmd = SIsMemberCommand()
        assert cmd.execute(ctx) == 1

    def test_sismember_nonexistent_member(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['sismember', 'myset', 'd']
        cmd = SIsMemberCommand()
        assert cmd.execute(ctx) == 0

    def test_sismember_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['sismember', 'nosuchkey', 'a']
        cmd = SIsMemberCommand()
        assert cmd.execute(ctx) == 0

    def test_sismember_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        ctx.cmdtokens = ['sismember', 'mystr', 'a']
        cmd = SIsMemberCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_sismember_invalid_syntax(self, ctx):
        ctx.cmdtokens = ['sismember']
        with pytest.raises(ValueError, match="sismember command requires key and member"):
            SIsMemberCommand().execute(ctx)


class TestSMembersCommand:
    def test_smembers_populated_set(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['smembers', 'myset']
        cmd = SMembersCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b', 'c'}

    def test_smembers_empty_set(self, ctx):
        ctx.db.set('myset', set())
        ctx.cmdtokens = ['smembers', 'myset']
        cmd = SMembersCommand()
        assert cmd.execute(ctx) == []

    def test_smembers_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['smembers', 'nosuchkey']
        cmd = SMembersCommand()
        assert cmd.execute(ctx) == []

    def test_smembers_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        ctx.cmdtokens = ['smembers', 'mystr']
        cmd = SMembersCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_smembers_invalid_syntax(self, ctx):
        ctx.cmdtokens = ['smembers']
        with pytest.raises(ValueError, match="smembers command requires key"):
            SMembersCommand().execute(ctx)


class TestSMIsMemberCommand:
    def test_smismember_all_existing(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['smismember', 'myset', 'a', 'b']
        cmd = SMIsMemberCommand()
        assert cmd.execute(ctx) == [1, 1]

    def test_smismember_mixed_existence(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['smismember', 'myset', 'a', 'd', 'b']
        cmd = SMIsMemberCommand()
        assert cmd.execute(ctx) == [1, 0, 1]

    def test_smismember_none_existing(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['smismember', 'myset', 'd', 'e']
        cmd = SMIsMemberCommand()
        assert cmd.execute(ctx) == [0, 0]

    def test_smismember_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['smismember', 'nosuchkey', 'a', 'b']
        cmd = SMIsMemberCommand()
        assert cmd.execute(ctx) == [0, 0]

    def test_smismember_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        ctx.cmdtokens = ['smismember', 'mystr', 'a', 'b']
        cmd = SMIsMemberCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_smismember_invalid_syntax(self, ctx):
        ctx.cmdtokens = ['smismember']
        with pytest.raises(ValueError, match="smismember command requires key and at least one member"):
            SMIsMemberCommand().execute(ctx)


class TestSMoveCommand:
    def test_smove_existing_member(self, ctx):
        ctx.db.set('source', {'a', 'b', 'c'})
        ctx.db.set('dest', {'d', 'e'})
        ctx.cmdtokens = ['smove', 'source', 'dest', 'b']
        cmd = SMoveCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('source') == {'a', 'c'}
        assert ctx.db.get('dest') == {'d', 'e', 'b'}

    def test_smove_to_empty_dest(self, ctx):
        ctx.db.set('source', {'a', 'b', 'c'})
        ctx.cmdtokens = ['smove', 'source', 'dest', 'b']
        cmd = SMoveCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('source') == {'a', 'c'}
        assert ctx.db.get('dest') == {'b'}

    def test_smove_last_member(self, ctx):
        ctx.db.set('source', {'a'})
        ctx.db.set('dest', {'b'})
        ctx.cmdtokens = ['smove', 'source', 'dest', 'a']
        cmd = SMoveCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists('source')  # Source set should be deleted
        assert ctx.db.get('dest') == {'a', 'b'}

    def test_smove_nonexistent_member(self, ctx):
        ctx.db.set('source', {'a', 'b'})
        ctx.db.set('dest', {'c'})
        ctx.cmdtokens = ['smove', 'source', 'dest', 'd']
        cmd = SMoveCommand()
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.get('source') == {'a', 'b'}
        assert ctx.db.get('dest') == {'c'}

    def test_smove_nonexistent_source(self, ctx):
        ctx.db.set('dest', {'a'})
        ctx.cmdtokens = ['smove', 'source', 'dest', 'b']
        cmd = SMoveCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_smove_wrong_type_source(self, ctx):
        ctx.db.set('source', 'string')
        ctx.db.set('dest', {'a'})
        ctx.cmdtokens = ['smove', 'source', 'dest', 'b']
        cmd = SMoveCommand()
        with pytest.raises(TypeError, match="source value is not a set"):
            cmd.execute(ctx)

    def test_smove_wrong_type_dest(self, ctx):
        ctx.db.set('source', {'a', 'b'})
        ctx.db.set('dest', 'string')
        ctx.cmdtokens = ['smove', 'source', 'dest', 'b']
        cmd = SMoveCommand()
        with pytest.raises(TypeError, match="destination value is not a set"):
            cmd.execute(ctx)

    def test_smove_invalid_syntax(self, ctx):
        ctx.cmdtokens = ['smove', 'source']
        with pytest.raises(ValueError, match="smove command requires source, destination and member"):
            SMoveCommand().execute(ctx)


class TestSPopCommand:
    def test_spop_single(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['spop', 'myset']
        cmd = SPopCommand()
        result = cmd.execute(ctx)
        assert result in {'a', 'b', 'c'}
        assert len(ctx.db.get('myset')) == 2
        assert result not in ctx.db.get('myset')

    def test_spop_multiple(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c', 'd'})
        ctx.cmdtokens = ['spop', 'myset', '2']
        cmd = SPopCommand()
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert set(result).issubset({'a', 'b', 'c', 'd'})
        assert not set(result).intersection(ctx.db.get('myset'))

    def test_spop_all_members(self, ctx):
        ctx.db.set('myset', {'a', 'b'})
        ctx.cmdtokens = ['spop', 'myset', '2']
        cmd = SPopCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b'}
        assert not ctx.db.exists('myset')  # Set should be deleted

    def test_spop_more_than_exists(self, ctx):
        ctx.db.set('myset', {'a', 'b'})
        ctx.cmdtokens = ['spop', 'myset', '5']
        cmd = SPopCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b'}
        assert not ctx.db.exists('myset')

    def test_spop_empty_set(self, ctx):
        ctx.db.set('myset', set())
        ctx.cmdtokens = ['spop', 'myset']
        cmd = SPopCommand()
        assert cmd.execute(ctx) is None

    def test_spop_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['spop', 'nosuchkey']
        cmd = SPopCommand()
        assert cmd.execute(ctx) is None

    def test_spop_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        ctx.cmdtokens = ['spop', 'mystr']
        cmd = SPopCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_spop_invalid_count(self, ctx):
        ctx.cmdtokens = ['spop', 'myset', '-1']
        with pytest.raises(ValueError, match="count must be positive"):
            SPopCommand().execute(ctx)


class TestSRandMemberCommand:
    def test_srandmember_single(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['srandmember', 'myset']
        cmd = SRandMemberCommand()
        result = cmd.execute(ctx)
        assert result in {'a', 'b', 'c'}
        assert ctx.db.get('myset') == {'a', 'b', 'c'}  # Set should remain unchanged

    def test_srandmember_multiple_distinct(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c', 'd'})
        ctx.cmdtokens = ['srandmember', 'myset', '2']
        cmd = SRandMemberCommand()
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert set(result).issubset({'a', 'b', 'c', 'd'})
        assert ctx.db.get('myset') == {'a', 'b', 'c', 'd'}

    def test_srandmember_multiple_with_repeats(self, ctx):
        ctx.db.set('myset', {'a', 'b'})
        ctx.cmdtokens = ['srandmember', 'myset', '-3']
        cmd = SRandMemberCommand()
        result = cmd.execute(ctx)
        assert len(result) == 3
        assert set(result).issubset({'a', 'b'})

    def test_srandmember_empty_set(self, ctx):
        ctx.db.set('myset', set())
        ctx.cmdtokens = ['srandmember', 'myset']
        cmd = SRandMemberCommand()
        assert cmd.execute(ctx) is None

    def test_srandmember_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['srandmember', 'nosuchkey']
        cmd = SRandMemberCommand()
        assert cmd.execute(ctx) is None

    def test_srandmember_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        ctx.cmdtokens = ['srandmember', 'mystr']
        cmd = SRandMemberCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)


class TestSRemCommand:
    def test_srem_existing_members(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c', 'd'})
        ctx.cmdtokens = ['srem', 'myset', 'b', 'c']
        cmd = SRemCommand()
        result = cmd.execute(ctx)
        assert result == 2
        assert ctx.db.get('myset') == {'a', 'd'}

    def test_srem_some_nonexistent(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        ctx.cmdtokens = ['srem', 'myset', 'b', 'd']
        cmd = SRemCommand()
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('myset') == {'a', 'c'}

    def test_srem_all_members(self, ctx):
        ctx.db.set('myset', {'a', 'b'})
        ctx.cmdtokens = ['srem', 'myset', 'a', 'b']
        cmd = SRemCommand()
        result = cmd.execute(ctx)
        assert result == 2
        assert not ctx.db.exists('myset')  # Set should be deleted

    def test_srem_nonexistent_key(self, ctx):
        ctx.cmdtokens = ['srem', 'nosuchkey', 'a']
        cmd = SRemCommand()
        result = cmd.execute(ctx)
        assert result == 0

    def test_srem_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        ctx.cmdtokens = ['srem', 'mystr', 'a']
        cmd = SRemCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_srem_invalid_syntax(self, ctx):
        ctx.cmdtokens = ['srem', 'myset']
        with pytest.raises(ValueError, match="srem command requires key and at least one member"):
            SRemCommand().execute(ctx)


class TestSUnionCommand:
    def test_sunion_two_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        ctx.db.set('set2', {'c', 'd', 'e'})
        ctx.cmdtokens = ['sunion', 'set1', 'set2']
        cmd = SUnionCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b', 'c', 'd', 'e'}

    def test_sunion_multiple_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('set2', {'b', 'c'})
        ctx.db.set('set3', {'c', 'd'})
        ctx.cmdtokens = ['sunion', 'set1', 'set2', 'set3']
        cmd = SUnionCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b', 'c', 'd'}

    def test_sunion_with_empty_set(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('set2', set())
        ctx.cmdtokens = ['sunion', 'set1', 'set2']
        cmd = SUnionCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b'}

    def test_sunion_nonexistent_key(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.cmdtokens = ['sunion', 'set1', 'nosuchkey']
        cmd = SUnionCommand()
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b'}

    def test_sunion_all_nonexistent(self, ctx):
        ctx.cmdtokens = ['sunion', 'nosuchkey1', 'nosuchkey2']
        cmd = SUnionCommand()
        result = cmd.execute(ctx)
        assert result == []

    def test_sunion_wrong_type(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('str1', 'string')
        ctx.cmdtokens = ['sunion', 'set1', 'str1']
        cmd = SUnionCommand()
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_sunion_invalid_syntax(self, ctx):
        ctx.cmdtokens = ['sunion']
        with pytest.raises(ValueError, match="sunion command requires at least one key"):
            SUnionCommand().execute(ctx)
