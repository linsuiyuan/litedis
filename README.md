# Litedis

Litedis 是一个轻量级的 模仿Redis 的本地实现，无需相应的服务器。它实现了和 Redis 类似的功能，支持基本的数据结构和操作。适合调试代码时，没有 redis 服务器或者不想连接 redis 服务器的情况下使用。

## 功能特点

- 实现了基础数据结构：
  - string
  - list
  - hash
  - set
  - zset
- 支持过期时间设置
- 支持持久化，包括 AOF 和 RDB、以及混合模式