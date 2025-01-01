from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from refactor2.server.main import LitedisServer
from refactor2.server.persistence import AOF
from refactor2.typing import PersistenceType


class TestLitedisServer:
    @pytest.fixture
    def temp_dir(self, tmp_path):
        return tmp_path

    @pytest.fixture
    def server(self, temp_dir):
        server = LitedisServer(temp_dir)
        yield server
        if server.aof:
            server.aof.close()

    def test_initialization_with_string_path(self, temp_dir):

        server = LitedisServer(str(temp_dir))

        assert isinstance(server.data_path, Path)
        assert server.data_path == temp_dir
        assert isinstance(server.aof, AOF)
        assert server.persistence_type == PersistenceType.MIXED

    def test_initialization_with_path_object(self, temp_dir):
        server = LitedisServer(temp_dir)

        assert server.data_path == temp_dir
        assert isinstance(server.aof, AOF)

    @pytest.mark.parametrize("persistence_type,should_have_aof", [
        (PersistenceType.MIXED, True),
        (PersistenceType.AOF, True),
        (PersistenceType.LDB, False),
        (PersistenceType.NONE, False),
    ])
    def test_aof_initialization_based_on_persistence_type(self, temp_dir, persistence_type, should_have_aof):

        server = LitedisServer(temp_dir, persistence_type=persistence_type)

        if should_have_aof:
            assert server.aof is not None
        else:
            assert server.aof is None

    def test_process_command_line(self, server):

        mock_result = "OK"
        with patch.object(server, '_execute_command_line', return_value=mock_result) as mock_execute:
            # Act
            result = server.process_command_line("db0", "set key value")

            # Assert
            assert result == mock_result
            mock_execute.assert_called_once_with("db0", "set key value")
            server.aof.log_command.assert_called_once_with("db0", "set key value")

    def test_execute_command_line(self, server):
        # Arrange
        dbname = "db0"
        cmdline = "set key value"
        mock_db = Mock()
        mock_command = Mock()

        with patch.object(server.dbmanager, 'get_or_create_db', return_value=mock_db) as mock_get_db, \
                patch('refactor2.server.main.parse_command_line_to_object', return_value=mock_command) as mock_parse:
            # Act
            server._execute_command_line(dbname, cmdline)

            # Assert
            mock_get_db.assert_called_once_with(dbname)
            mock_parse.assert_called_once_with(cmdline)
            mock_command.execute.assert_called_once()

    def test_replay_commands_from_aof(self, server):
        # Arrange
        commands = [
            ("db0", "set key1 value1"),
            ("db1", "set key2 value2")
        ]

        with patch.object(server.aof, 'load_commands', return_value=commands), \
                patch.object(server, '_execute_command_line') as mock_execute:
            # Act
            server.replay_commands_from_aof()

            # Assert
            assert mock_execute.call_count == 2
            mock_execute.assert_any_call("db0", "set key1 value1")
            mock_execute.assert_any_call("db1", "set key2 value2")

    def test_directory_creation(self, temp_dir):

        test_path = temp_dir / "subdir"
        assert not test_path.exists()

        LitedisServer(test_path)

        assert test_path.exists()
        assert test_path.is_dir()

    @pytest.mark.parametrize("persistence_type", [
        PersistenceType.MIXED,
        PersistenceType.AOF,
        PersistenceType.LDB,
        PersistenceType.NONE
    ])
    def test_persistence_type_setting(self, temp_dir, persistence_type):

        server = LitedisServer(temp_dir, persistence_type=persistence_type)

        assert server.persistence_type == persistence_type