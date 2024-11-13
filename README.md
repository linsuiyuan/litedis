# Litedis

Litedis 是一个轻量级的 模仿Redis 的本地实现，它实现了和 Redis 类似的功能，支持基本的数据结构和操作。适合调试代码时，没有 Redis 服务器或者不想连接 Redis 服务器的情况下使用。

## 功能特点

- 实现了基础数据结构：
  - STING
  - LIST
  - HASH
  - SET
  - ZSET
- 支持过期时间设置
- 支持持久化，包括 AOF 和 RDB、以及混合模式
- 简单轻量，数据存储在本地，无需服务器

## 使用示例

基本使用

```py
import time

from litedis import Litedis

with Litedis("litedis:///data/db") as db:
    db.set("key", "value", ex=1)
    assert db.get("key") == "value"

    time.sleep(1.1)
    assert not db.get("key")
```


## 存在问题

- 不支持多进程（以后也不打算支持，有多进程需求的建议使用 Redis）
- 还有一些 Redis 命令方法未实现
