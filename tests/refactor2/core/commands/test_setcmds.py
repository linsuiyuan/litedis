import pytest

from refactor2.core.command.base import CommandContext
from refactor2.core.command.setcmds import (
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
from refactor2.core.persistence.ldb import LitedisDB


@pytest.fixture
def db():
    return LitedisDB("test")


@pytest.fixture
def ctx(db):
    return CommandContext(db)


class TestSAddCommand:
    def test_sadd_new_set(self, ctx):
        cmd = SAddCommand(['sadd', 'myset', 'a', 'b', 'c'])
        result = cmd.execute(ctx)
        assert result == 3
        assert ctx.db.get('myset') == {'a', 'b', 'c'}

    def test_sadd_existing_set(self, ctx):
        ctx.db.set('myset', {'a', 'b'})
        cmd = SAddCommand(['sadd', 'myset', 'b', 'c'])
        result = cmd.execute(ctx)
        assert result == 1  # Only 'c' is new
        assert ctx.db.get('myset') == {'a', 'b', 'c'}

    def test_sadd_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        cmd = SAddCommand(['sadd', 'mystr', 'a'])
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_sadd_invalid_syntax(self):
        with pytest.raises(ValueError, match="sadd command requires key and at least one member"):
            SAddCommand(['sadd'])


class TestSCardCommand:
    def test_scard_empty_set(self, ctx):
        ctx.db.set('myset', set())
        cmd = SCardCommand(['scard', 'myset'])
        assert cmd.execute(ctx) == 0

    def test_scard_populated_set(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SCardCommand(['scard', 'myset'])
        assert cmd.execute(ctx) == 3

    def test_scard_nonexistent_key(self, ctx):
        cmd = SCardCommand(['scard', 'nosuchkey'])
        assert cmd.execute(ctx) == 0

    def test_scard_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        cmd = SCardCommand(['scard', 'mystr'])
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_scard_invalid_syntax(self):
        with pytest.raises(ValueError, match="scard command requires key"):
            SCardCommand(['scard'])


class TestSDiffCommand:
    def test_sdiff_two_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        ctx.db.set('set2', {'b', 'c', 'd'})
        cmd = SDiffCommand(['sdiff', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert set(result) == {'a'}

    def test_sdiff_multiple_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c', 'd'})
        ctx.db.set('set2', {'b'})
        ctx.db.set('set3', {'c'})
        cmd = SDiffCommand(['sdiff', 'set1', 'set2', 'set3'])
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'd'}

    def test_sdiff_nonexistent_first_key(self, ctx):
        cmd = SDiffCommand(['sdiff', 'nosuchkey', 'set2'])
        assert cmd.execute(ctx) == []

    def test_sdiff_nonexistent_other_key(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        cmd = SDiffCommand(['sdiff', 'set1', 'nosuchkey'])
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b', 'c'}

    def test_sdiff_wrong_type(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('str1', 'string')
        cmd = SDiffCommand(['sdiff', 'set1', 'str1'])
        with pytest.raises(TypeError, match="value at str1 is not a set"):
            cmd.execute(ctx)

    def test_sdiff_invalid_syntax(self):
        with pytest.raises(ValueError, match="sdiff command requires at least one key"):
            SDiffCommand(['sdiff'])


class TestSInterCommand:
    def test_sinter_two_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        ctx.db.set('set2', {'b', 'c', 'd'})
        cmd = SInterCommand(['sinter', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert set(result) == {'b', 'c'}

    def test_sinter_multiple_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c', 'd'})
        ctx.db.set('set2', {'b', 'c', 'd'})
        ctx.db.set('set3', {'b', 'c'})
        cmd = SInterCommand(['sinter', 'set1', 'set2', 'set3'])
        result = cmd.execute(ctx)
        assert set(result) == {'b', 'c'}

    def test_sinter_empty_intersection(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('set2', {'c', 'd'})
        cmd = SInterCommand(['sinter', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert result == []

    def test_sinter_nonexistent_key(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        cmd = SInterCommand(['sinter', 'set1', 'nosuchkey'])
        result = cmd.execute(ctx)
        assert result == []

    def test_sinter_wrong_type(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('str1', 'string')
        cmd = SInterCommand(['sinter', 'set1', 'str1'])
        with pytest.raises(TypeError, match="value at str1 is not a set"):
            cmd.execute(ctx)

    def test_sinter_invalid_syntax(self):
        with pytest.raises(ValueError, match="sinter command requires at least one key"):
            SInterCommand(['sinter'])


class TestSInterCardCommand:
    def test_sintercard_two_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        ctx.db.set('set2', {'b', 'c', 'd'})
        cmd = SInterCardCommand(['sintercard', '2', 'set1', 'set2'])
        assert cmd.execute(ctx) == 2  # {'b', 'c'}

    def test_sintercard_with_limit(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c', 'd'})
        ctx.db.set('set2', {'b', 'c', 'd'})
        cmd = SInterCardCommand(['sintercard', '2', 'set1', 'set2', 'LIMIT', '1'])
        assert cmd.execute(ctx) == 1

    def test_sintercard_empty_intersection(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('set2', {'c', 'd'})
        cmd = SInterCardCommand(['sintercard', '2', 'set1', 'set2'])
        assert cmd.execute(ctx) == 0

    def test_sintercard_nonexistent_key(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        cmd = SInterCardCommand(['sintercard', '2', 'set1', 'nosuchkey'])
        assert cmd.execute(ctx) == 0

    def test_sintercard_wrong_type(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('str1', 'string')
        cmd = SInterCardCommand(['sintercard', '2', 'set1', 'str1'])
        with pytest.raises(TypeError, match="value at str1 is not a set"):
            cmd.execute(ctx)

    def test_sintercard_invalid_numkeys(self):
        with pytest.raises(ValueError, match="numkeys must be positive"):
            SInterCardCommand(['sintercard', '-1', 'set1'])

    def test_sintercard_invalid_limit(self):
        with pytest.raises(ValueError, match="limit must be non-negative"):
            SInterCardCommand(['sintercard', '1', 'set1', 'LIMIT', '-1'])


class TestSIsMemberCommand:
    def test_sismember_existing_member(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SIsMemberCommand(['sismember', 'myset', 'b'])
        assert cmd.execute(ctx) == 1

    def test_sismember_nonexistent_member(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SIsMemberCommand(['sismember', 'myset', 'd'])
        assert cmd.execute(ctx) == 0

    def test_sismember_nonexistent_key(self, ctx):
        cmd = SIsMemberCommand(['sismember', 'nosuchkey', 'a'])
        assert cmd.execute(ctx) == 0

    def test_sismember_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        cmd = SIsMemberCommand(['sismember', 'mystr', 'a'])
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_sismember_invalid_syntax(self):
        with pytest.raises(ValueError, match="sismember command requires key and member"):
            SIsMemberCommand(['sismember'])


class TestSMembersCommand:
    def test_smembers_populated_set(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SMembersCommand(['smembers', 'myset'])
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b', 'c'}

    def test_smembers_empty_set(self, ctx):
        ctx.db.set('myset', set())
        cmd = SMembersCommand(['smembers', 'myset'])
        assert cmd.execute(ctx) == []

    def test_smembers_nonexistent_key(self, ctx):
        cmd = SMembersCommand(['smembers', 'nosuchkey'])
        assert cmd.execute(ctx) == []

    def test_smembers_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        cmd = SMembersCommand(['smembers', 'mystr'])
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_smembers_invalid_syntax(self):
        with pytest.raises(ValueError, match="smembers command requires key"):
            SMembersCommand(['smembers'])


class TestSMIsMemberCommand:
    def test_smismember_all_existing(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SMIsMemberCommand(['smismember', 'myset', 'a', 'b'])
        assert cmd.execute(ctx) == [1, 1]

    def test_smismember_mixed_existence(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SMIsMemberCommand(['smismember', 'myset', 'a', 'd', 'b'])
        assert cmd.execute(ctx) == [1, 0, 1]

    def test_smismember_none_existing(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SMIsMemberCommand(['smismember', 'myset', 'd', 'e'])
        assert cmd.execute(ctx) == [0, 0]

    def test_smismember_nonexistent_key(self, ctx):
        cmd = SMIsMemberCommand(['smismember', 'nosuchkey', 'a', 'b'])
        assert cmd.execute(ctx) == [0, 0]

    def test_smismember_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        cmd = SMIsMemberCommand(['smismember', 'mystr', 'a', 'b'])
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_smismember_invalid_syntax(self):
        with pytest.raises(ValueError, match="smismember command requires key and at least one member"):
            SMIsMemberCommand(['smismember', 'myset'])


class TestSMoveCommand:
    def test_smove_existing_member(self, ctx):
        ctx.db.set('source', {'a', 'b', 'c'})
        ctx.db.set('dest', {'d', 'e'})
        cmd = SMoveCommand(['smove', 'source', 'dest', 'b'])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('source') == {'a', 'c'}
        assert ctx.db.get('dest') == {'d', 'e', 'b'}

    def test_smove_to_empty_dest(self, ctx):
        ctx.db.set('source', {'a', 'b', 'c'})
        cmd = SMoveCommand(['smove', 'source', 'dest', 'b'])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('source') == {'a', 'c'}
        assert ctx.db.get('dest') == {'b'}

    def test_smove_last_member(self, ctx):
        ctx.db.set('source', {'a'})
        ctx.db.set('dest', {'b'})
        cmd = SMoveCommand(['smove', 'source', 'dest', 'a'])
        result = cmd.execute(ctx)
        assert result == 1
        assert not ctx.db.exists('source')  # Source set should be deleted
        assert ctx.db.get('dest') == {'a', 'b'}

    def test_smove_nonexistent_member(self, ctx):
        ctx.db.set('source', {'a', 'b'})
        ctx.db.set('dest', {'c'})
        cmd = SMoveCommand(['smove', 'source', 'dest', 'd'])
        result = cmd.execute(ctx)
        assert result == 0
        assert ctx.db.get('source') == {'a', 'b'}
        assert ctx.db.get('dest') == {'c'}

    def test_smove_nonexistent_source(self, ctx):
        ctx.db.set('dest', {'a'})
        cmd = SMoveCommand(['smove', 'source', 'dest', 'b'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_smove_wrong_type_source(self, ctx):
        ctx.db.set('source', 'string')
        ctx.db.set('dest', {'a'})
        cmd = SMoveCommand(['smove', 'source', 'dest', 'b'])
        with pytest.raises(TypeError, match="source value is not a set"):
            cmd.execute(ctx)

    def test_smove_wrong_type_dest(self, ctx):
        ctx.db.set('source', {'a', 'b'})
        ctx.db.set('dest', 'string')
        cmd = SMoveCommand(['smove', 'source', 'dest', 'b'])
        with pytest.raises(TypeError, match="destination value is not a set"):
            cmd.execute(ctx)

    def test_smove_invalid_syntax(self):
        with pytest.raises(ValueError, match="smove command requires source, destination and member"):
            SMoveCommand(['smove', 'source'])


class TestSPopCommand:
    def test_spop_single(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SPopCommand(['spop', 'myset'])
        result = cmd.execute(ctx)
        assert result in {'a', 'b', 'c'}
        assert len(ctx.db.get('myset')) == 2
        assert result not in ctx.db.get('myset')

    def test_spop_multiple(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c', 'd'})
        cmd = SPopCommand(['spop', 'myset', '2'])
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert set(result).issubset({'a', 'b', 'c', 'd'})
        assert not set(result).intersection(ctx.db.get('myset'))

    def test_spop_all_members(self, ctx):
        ctx.db.set('myset', {'a', 'b'})
        cmd = SPopCommand(['spop', 'myset', '2'])
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b'}
        assert not ctx.db.exists('myset')  # Set should be deleted

    def test_spop_more_than_exists(self, ctx):
        ctx.db.set('myset', {'a', 'b'})
        cmd = SPopCommand(['spop', 'myset', '5'])
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b'}
        assert not ctx.db.exists('myset')

    def test_spop_empty_set(self, ctx):
        ctx.db.set('myset', set())
        cmd = SPopCommand(['spop', 'myset'])
        assert cmd.execute(ctx) is None

    def test_spop_nonexistent_key(self, ctx):
        cmd = SPopCommand(['spop', 'nosuchkey'])
        assert cmd.execute(ctx) is None

    def test_spop_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        cmd = SPopCommand(['spop', 'mystr'])
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_spop_invalid_count(self):
        with pytest.raises(ValueError, match="count must be positive"):
            SPopCommand(['spop', 'myset', '-1'])


class TestSRandMemberCommand:
    def test_srandmember_single(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SRandMemberCommand(['srandmember', 'myset'])
        result = cmd.execute(ctx)
        assert result in {'a', 'b', 'c'}
        assert ctx.db.get('myset') == {'a', 'b', 'c'}  # Set should remain unchanged

    def test_srandmember_multiple_distinct(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c', 'd'})
        cmd = SRandMemberCommand(['srandmember', 'myset', '2'])
        result = cmd.execute(ctx)
        assert len(result) == 2
        assert set(result).issubset({'a', 'b', 'c', 'd'})
        assert ctx.db.get('myset') == {'a', 'b', 'c', 'd'}

    def test_srandmember_multiple_with_repeats(self, ctx):
        ctx.db.set('myset', {'a', 'b'})
        cmd = SRandMemberCommand(['srandmember', 'myset', '-3'])
        result = cmd.execute(ctx)
        assert len(result) == 3
        assert set(result).issubset({'a', 'b'})

    def test_srandmember_empty_set(self, ctx):
        ctx.db.set('myset', set())
        cmd = SRandMemberCommand(['srandmember', 'myset'])
        assert cmd.execute(ctx) is None

    def test_srandmember_nonexistent_key(self, ctx):
        cmd = SRandMemberCommand(['srandmember', 'nosuchkey'])
        assert cmd.execute(ctx) is None

    def test_srandmember_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        cmd = SRandMemberCommand(['srandmember', 'mystr'])
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)


class TestSRemCommand:
    def test_srem_existing_members(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c', 'd'})
        cmd = SRemCommand(['srem', 'myset', 'b', 'c'])
        result = cmd.execute(ctx)
        assert result == 2
        assert ctx.db.get('myset') == {'a', 'd'}

    def test_srem_some_nonexistent(self, ctx):
        ctx.db.set('myset', {'a', 'b', 'c'})
        cmd = SRemCommand(['srem', 'myset', 'b', 'd'])
        result = cmd.execute(ctx)
        assert result == 1
        assert ctx.db.get('myset') == {'a', 'c'}

    def test_srem_all_members(self, ctx):
        ctx.db.set('myset', {'a', 'b'})
        cmd = SRemCommand(['srem', 'myset', 'a', 'b'])
        result = cmd.execute(ctx)
        assert result == 2
        assert not ctx.db.exists('myset')  # Set should be deleted

    def test_srem_nonexistent_key(self, ctx):
        cmd = SRemCommand(['srem', 'nosuchkey', 'a'])
        result = cmd.execute(ctx)
        assert result == 0

    def test_srem_wrong_type(self, ctx):
        ctx.db.set('mystr', 'string')
        cmd = SRemCommand(['srem', 'mystr', 'a'])
        with pytest.raises(TypeError, match="value is not a set"):
            cmd.execute(ctx)

    def test_srem_invalid_syntax(self):
        with pytest.raises(ValueError, match="srem command requires key and at least one member"):
            SRemCommand(['srem', 'myset'])


class TestSUnionCommand:
    def test_sunion_two_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b', 'c'})
        ctx.db.set('set2', {'c', 'd', 'e'})
        cmd = SUnionCommand(['sunion', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b', 'c', 'd', 'e'}

    def test_sunion_multiple_sets(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('set2', {'b', 'c'})
        ctx.db.set('set3', {'c', 'd'})
        cmd = SUnionCommand(['sunion', 'set1', 'set2', 'set3'])
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b', 'c', 'd'}

    def test_sunion_with_empty_set(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('set2', set())
        cmd = SUnionCommand(['sunion', 'set1', 'set2'])
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b'}

    def test_sunion_nonexistent_key(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        cmd = SUnionCommand(['sunion', 'set1', 'nosuchkey'])
        result = cmd.execute(ctx)
        assert set(result) == {'a', 'b'}

    def test_sunion_all_nonexistent(self, ctx):
        cmd = SUnionCommand(['sunion', 'nosuchkey1', 'nosuchkey2'])
        result = cmd.execute(ctx)
        assert result == []

    def test_sunion_wrong_type(self, ctx):
        ctx.db.set('set1', {'a', 'b'})
        ctx.db.set('str1', 'string')
        cmd = SUnionCommand(['sunion', 'set1', 'str1'])
        with pytest.raises(TypeError, match="value at str1 is not a set"):
            cmd.execute(ctx)

    def test_sunion_invalid_syntax(self):
        with pytest.raises(ValueError, match="sunion command requires at least one key"):
            SUnionCommand(['sunion'])
