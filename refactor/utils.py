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