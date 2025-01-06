from unittest.mock import Mock

import pytest

from refactor2.core.command.base import CommandContext
from refactor2.core.command.basiccmds import SetCommand, GetCommand
from refactor2.core.persistence import LitedisDB


@pytest.fixture
def mock_db():
    db = Mock(spec=LitedisDB)
    db.exists.return_value = False
    db.get.return_value = None
    return db


@pytest.fixture
def command_context(mock_db):
    return CommandContext(db=mock_db)


class TestSetCommand:
    def test_basic_set(self, command_context):
        cmd = SetCommand(["set", "test_key", "test_value"])
        result = cmd.execute(command_context)

        command_context.db.set.assert_called_once_with("test_key", "test_value")  # noqa
        assert result == "OK"

    def test_set_with_nx_option_when_key_exists(self, command_context):
        command_context.db.exists.return_value = True
        cmd = SetCommand(["set", "test_key", "test_value", "nx"])

        result = cmd.execute(command_context)
        assert result is None
        command_context.db.set.assert_not_called()  # noqa

    def test_set_with_xx_option_when_key_not_exists(self, command_context):
        cmd = SetCommand(["set", "test_key", "test_value", "xx"])

        result = cmd.execute(command_context)
        assert result is None
        command_context.db.set.assert_not_called()  # noqa

    def test_set_with_get_option(self, command_context):
        command_context.db.get.return_value = "old_value"
        cmd = SetCommand(["set", "test_key", "new_value", "get"])

        result = cmd.execute(command_context)
        assert result == "old_value"
        command_context.db.set.assert_called_once_with("test_key", "new_value")  # noqa

    def test_set_with_expiration(self, command_context):
        cmd = SetCommand(["set", "test_key", "test_value", "pxat", "100"])
        cmd.execute(command_context)

        command_context.db.set_expiration.assert_called_once_with("test_key", 100)  # noqa

    def test_nx_xx_mutual_exclusion(self, command_context):
        # Test that NX and XX options cannot be used together
        cmd = SetCommand(["set", "test_key", "test_value", "nx", "xx"])

        with pytest.raises(ValueError, match="nx and xx are mutually exclusive"):
            cmd.execute(command_context)


class TestGetCommand:
    def test_get_existing_key(self, command_context):
        command_context.db.get.return_value = "test_value"
        cmd = GetCommand(["get", "test_key"])

        result = cmd.execute(command_context)
        assert result == "test_value"
        command_context.db.get.assert_called_once_with("test_key")  # noqa

    def test_get_non_existing_key(self, command_context):
        command_context.db.get.return_value = None
        cmd = GetCommand(["get", "non_existing_key"])

        result = cmd.execute(command_context)
        assert result is None
        command_context.db.get.assert_called_once_with("non_existing_key")  # noqa
