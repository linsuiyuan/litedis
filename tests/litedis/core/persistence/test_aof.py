import os
from pathlib import Path

import pytest

from litedis.core.persistence import AOF
from litedis.typing import DBCommandPair


@pytest.fixture
def temp_dir(tmp_path):
    # Create a temporary directory for testing
    return tmp_path


@pytest.fixture
def aof_file(temp_dir):
    # Create an AOF instance for testing
    aof = AOF(temp_dir, "test.aof")
    yield aof


class TestAOF:
    def test_init(self, temp_dir):
        aof = AOF(temp_dir, "test.aof")
        assert aof._filename == "test.aof"
        assert isinstance(aof.data_path, Path)
        assert aof._file is None

    def test_init_with_string_path(self, temp_dir):
        aof = AOF(str(temp_dir), "test.aof")
        assert isinstance(aof.data_path, Path)
        assert aof.data_path == temp_dir

    def test_get_or_create_file(self, aof_file):
        # Test file creation and retrieval
        file = aof_file.get_or_create_file()
        assert not file.closed
        assert file.mode == "a"

        # Test file is reused
        file2 = aof_file.get_or_create_file()
        assert file is file2

    def test_exists_file(self, aof_file):
        assert not aof_file.exists_file()
        aof_file.get_or_create_file()
        assert aof_file.exists_file()

    def test_log_command(self, aof_file):
        cmd = DBCommandPair("test_db", ["SET", "key", "value"])
        aof_file.log_command(cmd)

        # Verify file content
        with open(aof_file._file_path, "r") as f:
            content = f.read()
            assert content == f"'test_db',['SET', 'key', 'value']\n"

    def test_load_commands(self, aof_file):
        commands = [
            DBCommandPair("db1", ["SET", "key1", "value1"]),
            DBCommandPair("db2", ["SET", "key2", "value2"]),
        ]

        # Write test commands
        for cmd in commands:
            aof_file.log_command(cmd)

        # Read and verify commands
        loaded_commands = list(aof_file.load_commands())
        assert len(loaded_commands) == 2

        for original, loaded in zip(commands, loaded_commands):
            assert loaded.dbname == original.dbname
            assert loaded.cmdtokens == original.cmdtokens

    def test_load_commands_nonexistent_file(self, aof_file):
        commands = list(aof_file.load_commands())
        assert len(commands) == 0

    def test_rewrite_commands(self, aof_file):
        # Test rewriting commands
        original_commands = [
            DBCommandPair("db1", ["SET", "key1", "value1"]),
            DBCommandPair("db2", ["SET", "key2", "value2"])
        ]

        # Rewrite commands
        aof_file.rewrite_commands(original_commands)

        # Verify rewritten content
        loaded_commands = list(aof_file.load_commands())
        assert len(loaded_commands) == 2

        for i, cmd in enumerate(loaded_commands):
            assert cmd.dbname == original_commands[i].dbname
            assert cmd.cmdtokens == original_commands[i].cmdtokens

    def test_rewrite_commands_error_handling(self, aof_file, monkeypatch):
        def mock_replace(*args):  # noqa
            raise OSError("Mock error")

        monkeypatch.setattr(os, "replace", mock_replace)

        with pytest.raises(Exception, match="Failed to rewrite.*"):
            aof_file.rewrite_commands([DBCommandPair("db1", ["SET", "key1", "value1"])])

    def test_close(self, aof_file):
        file = aof_file.get_or_create_file()
        assert not file.closed

        aof_file.close_file()
        assert file.closed

    def test_auto_close_on_del(self, temp_dir):
        aof = AOF(temp_dir, "test.aof")
        file = aof.get_or_create_file()
        assert not file.closed

        del aof
        assert file.closed
