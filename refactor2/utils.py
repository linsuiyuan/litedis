import threading


def thread_safe_singleton(cls):
    _instances = {}
    _lock = threading.Lock()

    def get_instance(*args, **kwargs):
        with _lock:
            if cls not in _instances:
                _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    return get_instance


def parse_string_command(command_str):
    parts = []
    current_part = ''
    in_quotes = False

    for char in command_str:
        if char == '"' and not in_quotes:
            in_quotes = True
            continue
        elif char == '"' and in_quotes:
            in_quotes = False
            if current_part:
                parts.append(current_part)
                current_part = ''
            continue
        elif char.isspace() and not in_quotes:
            if current_part:
                parts.append(current_part)
                current_part = ''
            continue
        current_part += char

    if current_part:
        parts.append(current_part)

    if not parts:
        return None

    return parts
