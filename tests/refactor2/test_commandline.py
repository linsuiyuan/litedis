from refactor2.commandline import parse_command_line, combine_command_line


def test_parse_command_line_basic():
    # Test basic command parsing without quotes
    assert parse_command_line("get key1") == ["get", "key1"]
    assert parse_command_line("set key1 value1") == ["set", "key1", "value1"]


def test_parse_command_line_with_quotes():
    # Test parsing with quoted strings
    assert parse_command_line('set key "hello world"') == ["set", "key", "hello world"]
    assert parse_command_line('set "key with spaces" value') == ["set", "key with spaces", "value"]


def test_parse_command_line_empty():
    # Test empty input
    assert parse_command_line("") == []
    assert parse_command_line("   ") == []


def test_parse_command_line_complex():
    # Test more complex scenarios
    assert parse_command_line('set "key" "value with spaces"') == ["set", "key", "value with spaces"]
    assert parse_command_line('get "key with \\"escaped\\" quotes"') == ['get', 'key with "escaped" quotes']


def test_parse_command_line_with_json():
    # Test parsing command with JSON string
    json_cmd = 'set key1 {"name": "John", "age": 30}'
    assert parse_command_line(json_cmd) == ["set", "key1", '{"name": "John", "age": 30}']

    # Test parsing command with quoted JSON string
    json_cmd_quoted = 'set key1 "{\\"name\\": \\"John\\", \\"age\\": 30}"'
    print(json_cmd_quoted)
    assert parse_command_line(json_cmd_quoted) == ["set", "key1", '{"name": "John", "age": 30}']


def test_combine_command_line_with_json():
    # Test combining command with JSON string
    args = ["set", "key1", '{\"name\": \"John\", \"age\": 30}']
    expected = r'set key1 "{\"name\": \"John\", \"age\": 30}"'
    print(expected)
    assert combine_command_line(args) == expected


def test_combine_command_line_basic():
    # Test basic command combining
    assert combine_command_line(["get", "key1"]) == "get key1"
    assert combine_command_line(["set", "key1", "value1"]) == "set key1 value1"


def test_combine_command_line_with_spaces():
    # Test combining with spaces in arguments
    assert combine_command_line(["set", "my key", "my value"]) == 'set "my key" "my value"'
    assert combine_command_line(["get", "key with spaces"]) == 'get "key with spaces"'


def test_combine_command_line_empty():
    # Test empty input
    assert combine_command_line([]) == ""
    assert combine_command_line(None) == ""
