import pytest
import time
from refactor2.server.commands.parsers import (
    SetCommandParser,
    GetCommandParser,
    parse_command_line_to_command)
from refactor2.server.commands.commands import (
    SetCommand,
    GetCommand, Command)


class TestSetCommandParser:
    def test_basic_set(self):
        parser = SetCommandParser()
        command = parser.parse("SET mykey myvalue")

        assert isinstance(command, SetCommand)
        assert command.key == "mykey"
        assert command.value == "myvalue"
        assert command.options == {}

    def test_set_with_nx(self):
        parser = SetCommandParser()
        command = parser.parse("SET mykey myvalue NX")

        assert command.options.get("nx") is True

    def test_set_with_xx(self):
        parser = SetCommandParser()
        command = parser.parse("SET mykey myvalue XX")

        assert command.options.get("xx") is True

    def test_set_with_get(self):
        parser = SetCommandParser()
        command = parser.parse("SET mykey myvalue GET")

        assert command.options.get("get") is True

    def test_set_with_keepttl(self):
        parser = SetCommandParser()
        command = parser.parse("SET mykey myvalue KEEPTTL")

        assert command.options.get("keepttl") is True

    def test_set_with_ex(self):
        parser = SetCommandParser()
        current_time = time.time()
        command = parser.parse("SET mykey myvalue EX 10")

        assert abs(command.options["expiration"] - (int(current_time * 1000) + 10000)) <= 1000

    def test_set_with_px(self):
        # Test SET with PX option
        parser = SetCommandParser()
        current_time = time.time()
        command = parser.parse("SET mykey myvalue PX 1000")

        assert abs(command.options["expiration"] - (int(current_time * 1000) + 1000)) <= 1000

    def test_set_with_exat(self):
        parser = SetCommandParser()
        command = parser.parse("SET mykey myvalue EXAT 1000")

        assert command.options["expiration"] == 1000 * 1000

    def test_set_with_pxat(self):
        parser = SetCommandParser()
        command = parser.parse("SET mykey myvalue PXAT 1000")

        assert command.options["expiration"] == 1000

    def test_set_invalid_command(self):
        parser = SetCommandParser()
        with pytest.raises(ValueError, match="set command requires key and value"):
            parser.parse("SET mykey")


class TestGetCommandParser:
    def test_basic_get(self):
        parser = GetCommandParser()
        command = parser.parse("GET mykey")

        assert isinstance(command, GetCommand)
        assert command.key == "mykey"

    def test_get_invalid_command(self):
        parser = GetCommandParser()
        with pytest.raises(ValueError, match="get command requires key"):
            parser.parse("GET")


class TestParseCommandLineToCommand:
    def test_valid_command(self):
        command = parse_command_line_to_command("SET mykey myvalue")
        assert isinstance(command, Command)

    def test_unknown_command(self):
        with pytest.raises(ValueError, match="unknown command line:.*"):
            parse_command_line_to_command("UNKNOWN mykey")

    def test_case_insensitive_command(self):
        command = parse_command_line_to_command("set mykey myvalue")
        assert isinstance(command, Command)
        command = parse_command_line_to_command("SET mykey myvalue")
        assert isinstance(command, Command)
