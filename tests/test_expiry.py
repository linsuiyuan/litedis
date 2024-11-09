import time
import pytest
from litedis.expiry import Expiry


@pytest.fixture
def expiry():
    return Expiry({})


def test_is_expired(expiry):
    """测试键过期检查"""
    # 设置一个1秒后过期的键
    expiry.expires['test_key'] = time.time() + 1
    assert not expiry.is_expired('test_key')
    
    # 设置一个已经过期的键
    expiry.expires['expired_key'] = time.time() - 1
    assert expiry.is_expired('expired_key')
    
    # 测试不存在的键
    assert not expiry.is_expired('non_existent_key')


def test_check_expired(expiry):
    """测试check_expired方法"""
    
    def callback(key):
        expiry.expires.pop(key)
    
    # 测试未过期的键
    expiry.expires = {'future_key': time.time() + 1}
    assert not expiry.check_expired('future_key', callback)
    assert len(expiry.expires) == 1
    
    # 测试已过期的键
    expiry.expires = {'past_key': time.time() - 1}
    assert expiry.check_expired('past_key', callback)
    assert len(expiry.expires) == 0
    # assert expired_keys[0] == 'past_key'


def test_handle_expired_keys_task(expiry):
    """测试过期键后台处理任务"""
    
    def callback(*keys):
        for key in keys:
            expiry.expires.pop(key)
    
    # 添加一些测试键
    current_time = time.time()
    expiry.expires['expired1'] = current_time - 2
    expiry.expires['expired2'] = current_time - 1
    expiry.expires['valid'] = current_time + 10
    
    # 启动后台任务
    expiry.run_handle_expired_keys_task(callback)
    
    # 等待一小段时间让后台任务执行
    time.sleep(1.2)
    
    # 验证结果
    assert 'expired1' not in expiry.expires
    assert 'expired2' not in expiry.expires
    assert 'valid' in expiry.expires
