from threading import Thread
from unittest.mock import patch

import pytest

from refactor2.core.dbmanager import DBManager, _dbs  # noqa
from refactor2.core.persistence import LitedisDB


@pytest.fixture
def temp_dir(tmp_path_factory):
    """Create a unique temporary directory for each test"""
    return tmp_path_factory.mktemp("litedis")


@pytest.fixture(autouse=True)
def reset_singleton():
    setattr(DBManager, '_instances', {})
    _dbs.clear()
    yield


@pytest.fixture
def mock_aof():
    with patch('refactor2.core.persistence.AOF') as mock:
        yield mock


@pytest.fixture
def db_manager(temp_dir):
    manager = DBManager(temp_dir)
    yield manager


def test_get_or_create_db(db_manager):
    db1 = db_manager.get_or_create_db("test_db1")
    assert isinstance(db1, LitedisDB)
    assert db1.name == "test_db1"

    db2 = db_manager.get_or_create_db("test_db1")
    assert db1 is db2


def test_concurrent_db_creation(db_manager):
    db_instances = []

    def create_db():
        db_ = db_manager.get_or_create_db("test_concurrent_db")
        db_instances.append(db_)

    threads = [Thread(target=create_db) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    first_db = db_instances[0]
    for db in db_instances[1:]:
        assert db is first_db
