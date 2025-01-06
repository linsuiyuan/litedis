from pathlib import Path
from unittest.mock import patch

import pytest

from refactor2.core.dbcommand import DBCommandTokens
from refactor2.core.dbmanager import DBManager
from refactor2.core.persistence import LitedisDB


@pytest.fixture
def temp_dir(tmp_path_factory):
    """Create a unique temporary directory for each test"""
    return tmp_path_factory.mktemp("litedis")


@pytest.fixture(autouse=True)
def reset_singleton():
    setattr(DBManager, '_instances', {})
    yield


@pytest.fixture
def db_manager(temp_dir):
    manager = DBManager(temp_dir)
    yield manager


class TestDBManager:
    def test_singleton_pattern(self, temp_dir):
        # Test singleton pattern
        manager1 = DBManager(data_path=temp_dir, persistence_on=True)
        manager2 = DBManager(data_path="different_path", persistence_on=False)

        # Should return the same instance with original configuration
        assert manager1 is manager2
        assert manager2._persistence_on is True
        assert manager2._data_path == temp_dir

    def test_init_with_persistence(self, temp_dir):
        manager = DBManager(persistence_on=True, data_path=temp_dir)
        assert manager._persistence_on is True
        assert isinstance(manager._data_path, Path)
        assert manager._data_path.exists()
        assert manager._aof is not None

    def test_init_without_persistence(self):
        manager = DBManager(persistence_on=False)
        assert not hasattr(manager, '_persistence_on')
        assert not hasattr(manager, '_aof')

    def test_get_or_create_db(self, db_manager):
        # Test database creation and retrieval
        db1 = db_manager.get_or_create_db("test_db")
        assert isinstance(db1, LitedisDB)
        assert db1.name == "test_db"

        # Test db reuse
        db2 = db_manager.get_or_create_db("test_db")
        assert db1 is db2

        # Verify global _dbs dictionary
        db = db_manager.get_or_create_db("test_db")
        assert "test_db" == db.name
        assert db is db1

    def test_get_or_create_db_concurrent(self, db_manager):
        # Test concurrent database creation
        import threading

        def create_db():
            db = db_manager.get_or_create_db("concurrent_db")
            assert isinstance(db, LitedisDB)

        threads = [threading.Thread(target=create_db) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify only one database instance was created
        assert len([db_manager.get_or_create_db('concurrent_db')]) == 1

    def test_process_command_read(self, db_manager):
        # Test processing read command
        db = db_manager.get_or_create_db("test_db")
        db.set("key1", "value1")

        cmd = DBCommandTokens("test_db", "get key1")
        result = db_manager.process_command(cmd)
        assert result == "value1"

    def test_process_command_write(self, db_manager):
        # Test processing write command
        cmd = DBCommandTokens("test_db", "set key1 value1")
        result = db_manager.process_command(cmd)
        assert result == "OK"

        # Verify in global _dbs
        db = db_manager.get_or_create_db("test_db")
        assert "test_db" in db.name
        assert db.get("key1") == "value1"

    def test_process_command_write_with_persistence(self, db_manager, temp_dir):
        # Test write command with persistence
        cmd = DBCommandTokens("test_db", "set key1 value1")
        db_manager.process_command(cmd)

        # Verify command was logged to AOF
        assert db_manager._aof.exists_file()
        commands = list(db_manager._aof.load_commands())
        assert len(commands) == 1
        assert commands[0].dbname == "test_db"
        assert commands[0].cmdtokens == "set key1 value1"

        # Create new manager instance to test persistence
        DBManager._instance = None
        new_manager = DBManager(persistence_on=True, data_path=temp_dir)
        db = new_manager.get_or_create_db("test_db")
        assert db.get("key1") == "value1"

    @patch('refactor2.core.dbmanager.AOF')
    def test_replay_aof_commands(self, mock_aof_class, temp_dir):
        mock_aof = mock_aof_class.return_value
        mock_aof.exists_file.return_value = True
        mock_aof.load_commands.return_value = [
            DBCommandTokens("test_db", "set key1 value1"),
            DBCommandTokens("test_db", "set key2 value2")
        ]

        manager = DBManager(persistence_on=True, data_path=temp_dir)
        assert manager._replay_aof_commands() is True

        # Verify in global _dbs
        db = manager.get_or_create_db("test_db")
        assert "test_db" == db.name
        assert db.get("key1") == "value1"
        assert db.get("key2") == "value2"

