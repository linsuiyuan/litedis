import inspect
from typing import Iterable, List, Tuple, Union, Callable, Any, Dict
from urllib import parse


def list_or_args(keys: Union[str, Iterable[str]], args: Tuple[str, ...]) -> List[str]:
    """
    合并keys和args成的新列表
    """
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


def find_list_index(lst: List, item: Any, direction: str = "left"):
    """
    查找列表索引
    :param lst: 列表
    :param item: 要查找的元素
    :param direction: 查找的方向，默认从左查找
    :return:
    """
    if direction.lower() == "left":
        for index in range(len(lst)):
            if lst[index] == item:
                return index
        return -1

    else:
        for index in range(len(lst) - 1, -1, -1):
            if lst[index] == item:
                return index
        return -1


def combine_args_signature(method: Callable, *args, **kwargs):
    """将实参和函数签名组合在一起，获得一个完成的参数键值对"""
    sig = inspect.signature(method)

    # 获取函数签名初始化字典
    d = {
        k: v.default if v.default != inspect._empty else None  # noqa
        for k, v in sig.parameters.items()
        if k != "self"
    }

    # 使用位置实参替换默认值
    if args:
        vs = list(d.values())
        vs[:len(args)] = args
        d = dict(zip(d.keys(), vs))

    # 使用关键字实参替换默认值
    if kwargs:
        d.update(kwargs)
    return d


def combine_database_url(
        scheme: str,
        username: str = "",
        password: str = "",
        host: str = "",
        port: Union[str, int] = "",
        path: str = "",
        db: str = "db",
) -> str:
    if username and password:
        auth = f"{username}:{password}"
    elif username:
        auth = username
    elif password:
        auth = password
    else:
        auth = ""

    if host and port:
        ip = f"{host}:{port}"
    elif host:
        ip = host
    elif port:
        ip = f"localhost:{port}"
    else:
        ip = ""

    if path:
        dbpath = f"{path.lstrip('./|/').rstrip('/')}/{db}"
    else:
        dbpath = db

    url = (f"{scheme}://{auth+'@' if auth else ''}"
           f"{ip}/{dbpath}")

    return url


def parse_database_url(url: str) -> Dict:
    result = parse.urlparse(url)
    path, db = result.path.rsplit("/", maxsplit=1)
    return {
        "scheme": result.scheme,
        "username": result.username,
        "password": result.password,
        "host": result.hostname,
        "port": result.port,
        "path": path,
        "db": db
    }
