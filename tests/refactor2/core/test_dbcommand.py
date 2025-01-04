import time

import pytest

from refactor2.core.dbcommand import DBCommandLineConverter
from refactor2.core.persistence import LitedisDB
from refactor2.typing import DBCommandLine


@pytest.fixture
def mock_db():
    db = LitedisDB("test_db")
    yield db


class TestDBCommandLineConverter:
    def test_dbs_to_commands_basic(self, mock_db):
        mock_db.set("key1", "value1")
        mock_db.set("key2", "value2")

        dbs = {"test_db": mock_db}
        commands = list(DBCommandLineConverter.dbs_to_commands(dbs))

        assert len(commands) == 2
        assert any(cmd.dbname == "test_db" and cmd.cmdline == 'set key1 value1' for cmd in commands)
        assert any(cmd.dbname == "test_db" and cmd.cmdline == 'set key2 value2' for cmd in commands)

    def test_dbs_to_commands_with_expiration(self, mock_db):
        mock_db.set("key1", "value1")
        expiration = int(time.time() * 1000) + 10000  # 10 seconds from now
        mock_db.set_expiration("key1", expiration)

        dbs = {"test_db": mock_db}
        commands = list(DBCommandLineConverter.dbs_to_commands(dbs))

        assert len(commands) == 1
        cmd = commands[0]
        assert cmd.dbname == "test_db"
        assert cmd.cmdline.startswith('set key1 value1 pxat')
        assert str(expiration) in cmd.cmdline

    def test_convert_db_object_to_cmdline_basic(self, mock_db):
        mock_db.set("key1", "value1")
        cmdline = DBCommandLineConverter._convert_db_object_to_cmdline("key1", mock_db)
        assert cmdline == 'set key1 value1'

    def test_convert_db_object_to_cmdline_with_expiration(self, mock_db):
        mock_db.set("key1", "value1")
        expiration = int(time.time() * 1000) + 10000
        mock_db.set_expiration("key1", expiration)

        cmdline = DBCommandLineConverter._convert_db_object_to_cmdline("key1", mock_db)
        assert cmdline.startswith('set key1 value1 pxat')
        assert str(expiration) in cmdline

    def test_convert_db_object_to_cmdline_missing_key(self, mock_db):
        with pytest.raises(KeyError, match="'missing_key' doesn't exist"):
            DBCommandLineConverter._convert_db_object_to_cmdline("missing_key", mock_db)

    def test_convert_db_object_to_cmdline_unsupported_type(self, mock_db):
        mock_db._data["invalid_key"] = 123

        with pytest.raises(TypeError, match="the value type the key.*is not supported"):
            DBCommandLineConverter._convert_db_object_to_cmdline("invalid_key", mock_db)

    def test_commands_to_dbs_basic(self):
        commands = [
            DBCommandLine("db1", 'set key1 value1'),
            DBCommandLine("db2", 'set key2 value2')
        ]

        dbs = DBCommandLineConverter.commands_to_dbs(commands)

        assert len(dbs) == 2
        assert "db1" in dbs
        assert "db2" in dbs
        assert dbs["db1"].get("key1") == "value1"
        assert dbs["db2"].get("key2") == "value2"

    def test_commands_to_dbs_same_db(self):
        commands = [
            DBCommandLine("db1", 'set key1 value1'),
            DBCommandLine("db1", 'set key2 value2')
        ]

        dbs = DBCommandLineConverter.commands_to_dbs(commands)

        assert len(dbs) == 1
        assert "db1" in dbs
        assert dbs["db1"].get("key1") == "value1"
        assert dbs["db1"].get("key2") == "value2"
