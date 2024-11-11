import time
import pytest

from litedis import BaseLitedis, DataType
from litedis.expiry import Expiry


@pytest.fixture
def temp_dir(tmp_path):
    """创建临时目录用于测试"""
    return tmp_path


@pytest.fixture
def db(temp_dir):
    """创建db实例用于测试"""
    db = BaseLitedis()
    db.db_name = "test_db"
    db.data_dir = temp_dir
    db.data = {
        "key": "value"
    }
    db.data_types = {
        "key": DataType.STRING
    }
    db.expires = {}
    return db


@pytest.fixture
def expiry(db):
    return Expiry(db=db)


def test_is_expired(expiry):
    """测试键过期检查"""
    # 设置一个1秒后过期的键
    expiry.db.expires['test_key'] = time.time() + 1
    assert not expiry.is_expired('test_key')
    
    # 设置一个已经过期的键
    expiry.db.expires['expired_key'] = time.time() - 1
    assert expiry.is_expired('expired_key')
    
    # 测试不存在的键
    assert not expiry.is_expired('non_existent_key')


def test_check_expired(expiry):
    """测试check_expired方法"""
    
    def callback(key):
        expiry.db.expires.pop(key)
    
    # 测试未过期的键
    expiry.db.expires = {'future_key': time.time() + 1}
    assert not expiry.check_expired('future_key', callback)
    assert len(expiry.db.expires) == 1
    
    # 测试已过期的键
    expiry.db.expires = {'past_key': time.time() - 1}
    assert expiry.check_expired('past_key', callback)
    assert len(expiry.db.expires) == 0
    # assert expired_keys[0] == 'past_key'


def test_handle_expired_keys_task(expiry):
    """测试过期键后台处理任务"""
    
    def callback(*keys):
        for key in keys:
            expiry.db.expires.pop(key)
    
    # 添加一些测试键
    current_time = time.time()
    expiry.db.expires['expired1'] = current_time - 2
    expiry.db.expires['expired2'] = current_time - 1
    expiry.db.expires['valid'] = current_time + 10
    
    # 启动后台任务
    expiry.run_handle_expired_keys_task(callback)
    
    # 等待一小段时间让后台任务执行
    time.sleep(1.2)
    
    # 验证结果
    assert 'expired1' not in expiry.db.expires
    assert 'expired2' not in expiry.db.expires
    assert 'valid' in expiry.db.expires
