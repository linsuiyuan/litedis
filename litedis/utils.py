from typing import Tuple, List, Union, Iterable


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
