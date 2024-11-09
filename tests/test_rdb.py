import threading
import time

import pytest

from litedis.persistence import RDB


@pytest.fixture
def temp_dir(tmp_path):
    """创建临时目录用于测试"""
    return tmp_path


@pytest.fixture
def db_data():
    """测试用的数据库数据"""
    return {
        "key1": "value1",
        "key2": 123,
        "key3": ["list", "items"]
    }


@pytest.fixture
def rdb_instance(temp_dir, db_data):
    """创建RDB实例的fixture"""
    db_lock = threading.Lock()
    return RDB(
        db_name="test_db",
        data_dir=temp_dir,
        db_data=db_data,
        db_lock=db_lock,
        rdb_save_frequency=1,
        compression=True
    )


def test_save_and_read_rdb(rdb_instance, db_data):
    """测试RDB的保存和读取功能"""
    # 保存数据
    assert rdb_instance.save_rdb() is True

    # 验证文件是否存在
    assert rdb_instance.rdb_path.exists()

    # 读取数据并验证
    loaded_data = rdb_instance.read_rdb()
    assert loaded_data == db_data


def test_save_rdb_without_compression(temp_dir, db_data):
    """测试不使用压缩的RDB保存"""
    db_lock = threading.Lock()
    rdb = RDB(
        db_name="test_db",
        data_dir=temp_dir,
        db_data=db_data,
        db_lock=db_lock,
        compression=False
    )

    assert rdb.save_rdb() is True
    loaded_data = rdb.read_rdb()
    assert loaded_data == db_data


def test_background_save(rdb_instance, db_data):
    """测试后台保存功能"""
    rdb_instance.save_task_in_background()
    # 等待后台任务执行
    time.sleep(2)

    # 验证文件是否已创建
    assert rdb_instance.rdb_path.exists()

    # 验证数据是否正确
    loaded_data = rdb_instance.read_rdb()
    assert loaded_data == db_data


def test_read_nonexistent_rdb(rdb_instance):
    """测试读取不存在的RDB文件"""
    # 确保文件不存在
    if rdb_instance.rdb_path.exists():
        rdb_instance.rdb_path.unlink()

    # 读取不存在的文件应该返回None
    assert rdb_instance.read_rdb() is None


def test_save_rdb_with_invalid_data(temp_dir):
    """测试保存无效数据时的错误处理"""
    db_lock = threading.Lock()
    # 创建一个包含无法序列化对象的数据
    invalid_data = {"key": lambda x: x}  # lambda函数无法被pickle序列化

    rdb = RDB(
        db_name="test_db",
        data_dir=temp_dir,
        db_data=invalid_data,
        db_lock=db_lock
    )

    with pytest.raises(Exception) as exc_info:
        rdb.save_rdb()
    assert "保存文件出错" in str(exc_info.value)
