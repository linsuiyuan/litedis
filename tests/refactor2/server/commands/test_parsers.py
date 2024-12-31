import time

import pytest
from refactor2.server.commands.parsers import SetCommandParser, parse_command_line_to_object
from refactor2.server.commands.commands import SetCommand


class TestSetCommandParser:
    def test_set_command_basic(self):
        parser = SetCommandParser()
        command = parser.parse("set key value")
        
        assert isinstance(command, SetCommand)
        assert command.key == "key"
        assert command.value == "value"
        assert command.options == {}

    def test_set_command_with_nx_option(self):
        parser = SetCommandParser()
        command = parser.parse("set key value nx")
        
        assert command.key == "key"
        assert command.value == "value"
        assert command.options == {"nx": True}

    def test_set_command_with_xx_option(self):
        parser = SetCommandParser()
        command = parser.parse("set key value xx")
        
        assert command.key == "key"
        assert command.value == "value"
        assert command.options == {"xx": True}

    def test_set_command_with_get_option(self):
        parser = SetCommandParser()
        command = parser.parse("set key value get")
        
        assert command.key == "key"
        assert command.value == "value"
        assert command.options == {"get": True}

    def test_set_command_with_keepttl_option(self):
        parser = SetCommandParser()
        command = parser.parse("set key value keepttl")
        
        assert command.key == "key"
        assert command.value == "value"
        assert command.options == {"keepttl": True}

    def test_set_command_with_ex_option(self):
        parser = SetCommandParser()
        command = parser.parse("set key value ex 10")
        
        assert command.key == "key"
        assert command.value == "value"
        assert "expiration" in command.options
        assert command.options["expiration"] > (time.time() + 8)* 1000

    def test_set_command_with_px_option(self):
        parser = SetCommandParser()
        command = parser.parse("set key value px 10000")
        
        assert command.key == "key"
        assert command.value == "value"
        assert "expiration" in command.options
        assert command.options["expiration"] > (time.time() + 8)* 1000

    def test_set_command_with_multiple_options(self):
        parser = SetCommandParser()
        command = parser.parse("set key value nx get")
        
        assert command.key == "key"
        assert command.value == "value"
        assert command.options == {"nx": True, "get": True}

    def test_set_command_invalid_syntax(self):
        parser = SetCommandParser()
        
        with pytest.raises(ValueError, match="set command requires key and value"):
            parser.parse("set key")


class TestCommandLineParser:
    def test_parse_command_line_to_object_set(self):
        command = parse_command_line_to_object("set key value")
        
        assert isinstance(command, SetCommand)
        assert command.key == "key"
        assert command.value == "value"

    def test_parse_command_line_to_object_unknown(self):
        with pytest.raises(ValueError, match="unknown command line:"):
            parse_command_line_to_object("unknown key value")



