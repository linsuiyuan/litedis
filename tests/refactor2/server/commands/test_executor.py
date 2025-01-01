from unittest.mock import Mock
from refactor2.server.commands import CommandContext
from refactor2.server.commands.executor import CommandExecutor
from refactor2.server.persistence import LitedisDB


def test_execute_set_command():
    mock_db = Mock(spec=LitedisDB)
    mock_context = CommandContext(mock_db)
    executor = CommandExecutor()

    result = executor.execute("set key value ex 10", mock_context)

    mock_db.set.assert_called_once_with("key", "value")
    mock_db.delete_expiration.assert_called_once_with("key")

    assert result == "OK"
