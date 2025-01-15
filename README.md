# Litedis



`Litedis` 是一个类似 Redis 的轻量级的本地的 NoSQL 数据库，支持基本的数据结构和操作。和 Redis 最大的不同是，Litedis 开箱即用，无需另开服务器进程。



## 特性

- 实现了基础数据结构及相关操作：
  - STING
  - LIST
  - HASH
  - SET
  - ZSET
- 支持设置过期时间
- 支持 AOF 持久化



## 安装和支持版本

- 使用 pip 安装

```sh
pip install litedis
```

- 支持的 Python 版本

  支持 Python3.8+
  
  

## 使用示例


### 持久化和数据库设置

- Litedis 是默认开启持久化的，可以通过参数进行相关设置。
- Litedis 可以将数据存储到不同的数据库下，可以通过 dbname 参数设置

```python
from litedis import Litedis

# 关闭持久化
litedis = Litedis(persistence_on=False)

# 设置持久化路径
litedis = Litedis(data_path="path")

# 设置数据库名称
litedis = Litedis(dbname="litedis")
```

### STRING 的使用


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
litedis.set("db", "litedis", px=100)  # 100毫秒后过期
assert litedis.get("db") == "litedis"
time.sleep(0.11)
assert litedis.get("db") is None
```

### LIST 的使用


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
assert not litedis.exists("list")  # 当所有元素被弹出后，相应的 List键 会自动删除
```

### Hash 的使用


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

### SET 的使用


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

### ZSET 的使用


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


