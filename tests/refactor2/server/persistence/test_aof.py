import pytest

from refactor2.server.persistence.aof import AOF


class TestAOF:

    @pytest.fixture(autouse=True)
    def setup_method(self, tmp_path):
        self.filename = "litedis.aof"
        self.tmp_path = tmp_path

    @pytest.fixture
    def aof(self):
        aof_instance = AOF(self.tmp_path)
        yield aof_instance
        del aof_instance

    def test_get_or_create_file(self, aof):
        file = aof.get_or_create_file()

        assert file is not None
        assert not file.closed
        assert (self.tmp_path / self.filename).exists()

    def test_log_command(self, aof):
        dbname = "db0"
        cmdline = "set key value"

        aof.log_command(dbname, cmdline)

        with open(self.tmp_path / self.filename, "r") as f:
            content = f.read()
            assert content == f"{dbname}/{cmdline}\n"

    def test_load_commands(self, aof):
        commands = [
            ("db0", "set key1 value1"),
            ("db1", "set key2 value2"),
            ("db0", "del key1")
        ]

        # Write test data
        with open(self.tmp_path / self.filename, "w") as f:
            for dbname, cmd in commands:
                f.write(f"{dbname}/{cmd}\n")

        loaded_commands = list(aof.load_commands())

        assert loaded_commands == commands

    def test_load_commands_with_file_not_exists(self, aof):

        loaded_commands = list(aof.load_commands())

        assert loaded_commands == []

    def test_close(self, aof):
        file = aof.get_or_create_file()
        assert not file.closed

        aof.close()

        assert file.closed

    def test_file_is_flushed_after_write(self, aof):
        dbname = "db0"
        cmdline = "set key value"

        aof.log_command(dbname, cmdline)

        # Can read immediately after write
        with open(self.tmp_path / self.filename, "r") as f:
            content = f.read()
            assert content == f"{dbname}/{cmdline}\n"
