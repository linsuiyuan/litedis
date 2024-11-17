from typing import Iterable, List, Tuple, Union


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
