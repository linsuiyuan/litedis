import inspect
from typing import Iterable, List, Tuple, Union, Callable


def list_or_args(keys: Union[str, Iterable[str]], args: Tuple[str, ...]) -> List[str]:
    # 返回一个合并keys和args后的新列表
    try:
        iter(keys)
        # 如果keys不是作为列表传递的，例如是一个字符串
        if isinstance(keys, str):
            keys = [keys]
        else:
            keys = list(keys)
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


def find_index_from_left(lst, value):
    for index in range(len(lst)):
        if lst[index] == value:
            return index
    return -1


def find_index_from_right(lst, value):
    for index in range(len(lst) - 1, -1, -1):
        if lst[index] == value:
            return index
    return -1


def combine_args_signature(method: Callable, *args, **kwargs):
    """将实参和函数签名组合在一起，获得一个完成的参数键值对"""
    sig = inspect.signature(method)
    d = {
        k: v.default if v.default != inspect._empty else None  # noqa
        for k, v in sig.parameters.items()
        if k != "self"
    }
    if args:
        vs = list(d.values())
        vs[:len(args)] = args
        d = dict(zip(d.keys(), vs))
    if kwargs:
        d.update(kwargs)
    return d
