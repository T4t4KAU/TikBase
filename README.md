# 分布式KV存储系统

基于 Go 开发的分布式键值存储系统，目前已经具有如下特性：
1. 支持两种存储引擎，在系统中命名为 bases 和 caches
    - bases: 基于 Bitcask 设计的存储引擎 
        - 可采用 自适应基数树/跳表 作为内存索引
        - 使用 日志文件 持久化数据 可保证数据的持久性和一致性
        - 适用于读多写少的场景，对于大量的写操作，可以提供高吞吐量和低延迟
        - 支持单机事务
    - caches: 基于 HashMap 设计的存储引擎
        - 数据完全存储在内存
        - 自动回收失效数据
        - 适用于内存存储场景
2. 高性能网络IO: 基于 Reactor 模型，支持高并发处理请求
3. 支持 客户端使用 HTTP协议 和 Redis协议 访问系统
4. 消息队列: 基于 P/S 模型，主要用于日志压缩和处理
5. 基于令牌桶实现限流，实现对系统的保护

即将支持:
1. 分布式 多副本存储

服务端启动方式(占用9096端口)：
```bash
go run main.go
```

客户端启动方式：
```bash
go run client/main.go
```

在客户端中命令行操作：
```bash
 _____ _ _    ____                 
|_   _(_) | _| __ )  __ _ ___  ___ 
  | | | | |/ /  _ \ / _` / __|/ _ \
  | | | |   <| |_) | (_| \__ \  __/
  |_| |_|_|\_\____/ \__,_|___/\___|

connecting to:  127.0.0.1:9096
> set key value   # 设置键值对
[OK]
> get key
value
> expire key 3   # 设置过期时间
[OK]
> del key        # 删除键值对
[OK]
> get key        # 获取键值对
[KEY NOT FOUND]
```

