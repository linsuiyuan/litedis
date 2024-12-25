import pytest  # noqa
from litedis.utils import (
    list_or_args,
    find_list_index,
    combine_args_signature,
    combine_database_url,
    parse_database_url
)


def test_list_or_args():
    # 测试单个字符串key
    assert list_or_args("key1", ()) == ["key1"]
    
    # 测试字符串key和额外参数
    assert list_or_args("key1", ("key2", "key3")) == ["key1", "key2", "key3"]
    
    # 测试列表key
    assert list_or_args(["key1", "key2"], ()) == ["key1", "key2"]
    
    # 测试元组key
    assert list_or_args(("key1", "key2"), ()) == ["key1", "key2"]


def test_find_list_index():
    test_list = [1, 2, 3, 2, 4]
    
    # 测试从左查找
    assert find_list_index(test_list, 2, "left") == 1
    
    # 测试从右查找
    assert find_list_index(test_list, 2, "right") == 3
    
    # 测试找不到的情况
    assert find_list_index(test_list, 5, "left") == -1
    assert find_list_index(test_list, 5, "right") == -1


def test_combine_args_signature():
    def test_func(a, b=2, c=3):  # noqa
        pass
    
    # 测试只有位置参数
    result = combine_args_signature(test_func, 1)
    assert result == {"a": 1, "b": 2, "c": 3}
    
    # 测试位置参数和关键字参数
    result = combine_args_signature(test_func, 1, b=5)
    assert result == {"a": 1, "b": 5, "c": 3}
    
    # 测试所有参数都使用关键字参数
    result = combine_args_signature(test_func, a=1, b=2, c=5)
    assert result == {"a": 1, "b": 2, "c": 5}


def test_combine_database_url():
    # 测试完整URL
    url = combine_database_url(
        scheme="redis",
        username="user",
        password="pass",
        host="localhost",
        port=6379,
        path="/data",
        db="0"
    )
    assert url == "redis://user:pass@localhost:6379/data/0"
    
    # 测试最小URL
    url = combine_database_url(scheme="redis")
    assert url == "redis:///db"
    
    # 测试没有认证信息的URL
    url = combine_database_url(
        scheme="redis",
        host="localhost",
        port=6379
    )
    assert url == "redis://localhost:6379/db"


def test_parse_database_url():
    # 测试完整URL解析
    url = "redis://user:pass@localhost:6379/data/0"
    result = parse_database_url(url)
    assert result == {
        "scheme": "redis",
        "username": "user",
        "password": "pass",
        "host": "localhost",
        "port": 6379,
        "path": "/data",
        "db": "0"
    }
    
    # 测试最小URL解析
    url = "redis:///db"
    result = parse_database_url(url)
    assert result == {
        "scheme": "redis",
        "username": None,
        "password": None,
        "host": None,
        "port": None,
        "path": "",
        "db": "db"
    }
