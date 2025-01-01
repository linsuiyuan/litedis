import pytest

from refactor2.server.commands.commands import SetCommand
from refactor2.server.commands import CommandContext, CommandExecutionMode


class TestSetCommand:
    @pytest.fixture
    def mock_db(self, mocker):
        db = mocker.Mock()
        db.exists.return_value = False
        db.get.return_value = None
        return db

    @pytest.fixture
    def ctx(self, mock_db):
        return CommandContext(CommandExecutionMode.NORMAL, mock_db)

    def test_basic_set_without_options(self, ctx):
        command = SetCommand("key", "value")
        result = command.execute(ctx)

        assert result == "OK"
        ctx.db.set.assert_called_once_with("key", "value")
        ctx.db.delete_expiration.assert_called_once_with("key")

    def test_set_with_nx_when_key_not_exists(self, ctx):
        ctx.db.exists.return_value = False
        command = SetCommand("key", "value", {"nx": True})
        
        result = command.execute(ctx)
        
        assert result == "OK"
        ctx.db.set.assert_called_once_with("key", "value")

    def test_set_with_nx_when_key_exists(self, ctx):
        ctx.db.exists.return_value = True
        command = SetCommand("key", "value", {"nx": True})
        
        result = command.execute(ctx)
        
        assert result is None
        ctx.db.set.assert_not_called()

    def test_set_with_xx_when_key_exists(self, ctx):
        ctx.db.exists.return_value = True
        command = SetCommand("key", "value", {"xx": True})
        
        result = command.execute(ctx)
        
        assert result == "OK"
        ctx.db.set.assert_called_once_with("key", "value")

    def test_set_with_xx_when_key_not_exists(self, ctx):
        ctx.db.exists.return_value = False
        command = SetCommand("key", "value", {"xx": True})
        
        result = command.execute(ctx)
        
        assert result is None
        ctx.db.set.assert_not_called()

    def test_set_with_get(self, ctx):
        ctx.db.get.return_value = "old_value"
        command = SetCommand("key", "new_value", {"get": True})
        
        result = command.execute(ctx)
        
        assert result == "old_value"
        ctx.db.set.assert_called_once_with("key", "new_value")

    def test_set_with_keepttl(self, ctx):
        command = SetCommand("key", "value", {"keepttl": True})
        
        result = command.execute(ctx)
        
        assert result == "OK"
        ctx.db.set.assert_called_once_with("key", "value")
        ctx.db.delete_expiration.assert_not_called()

    def test_set_with_expiration(self, ctx):
        expiration = 1000
        command = SetCommand("key", "value", {"expiration": expiration})
        
        result = command.execute(ctx)
        
        assert result == "OK"
        ctx.db.set.assert_called_once_with("key", "value")
        ctx.db.set_expiration.assert_called_once_with("key", expiration)

    def test_set_with_nx_and_xx_raises_error(self, ctx):
        command = SetCommand("key", "value", {"nx": True, "xx": True})
        
        with pytest.raises(ValueError, match="nx and xx are mutually exclusive"):
            command.execute(ctx)

    def test_set_with_multiple_options(self, ctx):
        expiration = 1000
        command = SetCommand("key", "value", {
            "get": True,
            "keepttl": True,
            "expiration": expiration
        })
        ctx.db.get.return_value = "old_value"
        
        result = command.execute(ctx)
        
        assert result == "old_value"
        ctx.db.set.assert_called_once_with("key", "value")
        ctx.db.delete_expiration.assert_not_called()
        ctx.db.set_expiration.assert_called_once_with("key", expiration)
