Other Language Version：

- [中文](README.md)

# Litedis


Litedis is a lightweight, local NoSQL database similar to Redis, implemented in Python. 
It supports basic data structures and operations. 
The main difference from Redis is that Litedis is ready to use out of the box without requiring an additional server process.


## Features

- Implemented basic data structures and related operations：
  - STING
  - LIST
  - HASH
  - SET
  - ZSET
- Supports setting expiration times
- Supports AOF persistence


## Installation and Supported Versions

- Install using pip

```sh
pip install litedis
```

- Supported Python versions

  Supports Python 3.8+
  

## Usage Examples


### Persistence and Database Settings

- Litedis has persistence enabled by default and can be configured with parameters.
- Litedis can store data in different databases, which can be set via the dbname parameter.

```python
from litedis import Litedis

# Disable persistence
litedis = Litedis(persistence_on=False)

# Set persistence path
litedis = Litedis(data_path="path")

# Set database name
litedis = Litedis(dbname="litedis")
```

### Using STRING


```python
import time

from litedis import Litedis

litedis = Litedis()

# set and get
litedis.set("db", "litedis")
assert litedis.get("db") == "litedis"

# delete
litedis.delete("db")
assert litedis.get("db") is None

# expiration
litedis.set("db", "litedis", px=100)  # expires in 100 milliseconds
assert litedis.get("db") == "litedis"
time.sleep(0.11)
assert litedis.get("db") is None
```

### Using LIST


```python
from litedis import Litedis

litedis = Litedis()

# lpush
litedis.lpush("list", "a", "b", "c")
assert litedis.lrange("list", 0, -1) == ["c", "b", "a"]
litedis.delete("list")

# rpush
litedis.rpush("list", "a", "b", "c")
assert litedis.lrange("list", 0, -1) == ["a", "b", "c"]
litedis.delete("list")

# lpop
litedis.lpush("list", "a", "b")
assert litedis.lpop("list") == "b"
assert litedis.lpop("list") == "a"
assert litedis.lrange("list", 0, -1) == []
assert not litedis.exists("list")  # The List key is automatically deleted when all elements are popped
```

### Using HASH


```python
from litedis import Litedis

litedis = Litedis()
litedis.delete("hash")

# hset
litedis.hset("hash", {"key1":"value1", "key2":"value2"})
assert litedis.hget("hash", "key1") == "value1"

# hkeys and hvals
assert litedis.hkeys("hash") == ["key1", "key2"]
assert litedis.hvals("hash") == ["value1", "value2"]
```

### Using SET


```python
from litedis import Litedis

litedis = Litedis()
litedis.delete("set", "set1", "set2")

# sadd
litedis.sadd("set", "a")
litedis.sadd("set", "b", "c")
members = litedis.smembers("set")
assert set(members) == {"a", "b", "c"}

litedis.sadd("set1", "a", "b", "c")
litedis.sadd("set2", "b", "c", "d")

# inter
result = litedis.sinter("set1", "set2")
assert set(result) == {"b", "c"}

# union
result = litedis.sunion("set1", "set2")
assert set(result) == {"a", "b", "c", "d"}

# diff
result = litedis.sdiff("set1", "set2")
assert set(result) == {"a"}
```

### Using ZSET


```python
from litedis import Litedis

litedis = Litedis()
litedis.delete("zset")

# zadd
litedis.zadd("zset", {"a": 1, "b": 2, "c": 3})
assert litedis.zscore("zset", "a") == 1

# zrange
assert litedis.zrange("zset", 0, -1) == ["a", "b", "c"]

# zcard
assert litedis.zcard("zset") == 3

# zscore
assert litedis.zscore("zset", "a") == 1
```


