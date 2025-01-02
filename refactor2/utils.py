import threading
from abc import ABCMeta


def thread_safe_singleton(cls):
    _instances = {}
    _lock = threading.Lock()

    def get_instance(*args, **kwargs):
        if cls not in _instances:
            with _lock:
                if cls not in _instances:
                    _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    return get_instance


class _SingletonMeta(type):
    _instances = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class SingletonMeta(_SingletonMeta, ABCMeta): ...


def parse_command_line(cmdline):
    parts = []
    current_part = ''
    in_quotes = False

    for char in cmdline:
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
