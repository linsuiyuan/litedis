from unittest.mock import Mock

import pytest

from refactor2.client.commands import BasicCommands


class TestBasicCommands:
    @pytest.fixture
    def command(self):
        # Create a mock instance with execute_command method
        cmd = BasicCommands()
        cmd.execute = Mock()
        return cmd

    def test_get_simple(self, command):
        # Test basic get operation
        command.get("test_key")
        command.execute.assert_called_once_with("get", "test_key")

    def test_set_simple(self, command):
        # Test basic set operation with only key and value
        command.set("test_key", "test_value")
        command.execute.assert_called_once_with("set", "test_key", "test_value")

    def test_set_with_ex(self, command):
        # Test set with expiration in seconds
        command.set("test_key", "test_value", ex=60)
        command.execute.assert_called_once_with("set", "test_key", "test_value", "ex", 60)

    def test_set_with_px(self, command):
        # Test set with expiration in milliseconds
        command.set("test_key", "test_value", px=1000)
        command.execute.assert_called_once_with("set", "test_key", "test_value", "px", 1000)

    def test_set_with_nx(self, command):
        # Test set with NX flag (only set if key does not exist)
        command.set("test_key", "test_value", nx=True)
        command.execute.assert_called_once_with("set", "test_key", "test_value", "nx")

    def test_set_with_xx(self, command):
        # Test set with XX flag (only set if key exists)
        command.set("test_key", "test_value", xx=True)
        command.execute.assert_called_once_with("set", "test_key", "test_value", "xx")

    def test_set_with_keepttl(self, command):
        # Test set with keepttl flag
        command.set("test_key", "test_value", keepttl=True)
        command.execute.assert_called_once_with("set", "test_key", "test_value", "keepttl")

    def test_set_with_get(self, command):
        # Test set with get flag
        command.set("test_key", "test_value", get=True)
        command.execute.assert_called_once_with("set", "test_key", "test_value", "get")

    def test_set_with_exat(self, command):
        # Test set with absolute Unix time expiration in seconds
        command.set("test_key", "test_value", exat=1234567890)
        command.execute.assert_called_once_with("set", "test_key", "test_value", "exat", 1234567890)

    def test_set_with_pxat(self, command):
        # Test set with absolute Unix time expiration in milliseconds
        command.set("test_key", "test_value", pxat=1234567890000)
        command.execute.assert_called_once_with("set", "test_key", "test_value", "pxat", 1234567890000)

    def test_set_with_multiple_options(self, command):
        # Test set with multiple options combined
        command.set("test_key", "test_value", ex=60, nx=True, get=True)
        command.execute.assert_called_once_with(
            "set", "test_key", "test_value", "ex", 60, "nx", "get"
        )