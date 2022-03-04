## MyProxy 
"MyProxy"是一个分布式数据库中间件，主要用于提供字段级加密以及基于多租户架构的垂直分表功能。
该代理目前已支持基础的mysql协议（暂时还不支持prepare statement），能够让应用程序无缝迁移到该代理服务上，而且DBA
能够通过MySQL客户端直接连接该代理服务。相比于其他的数据库代理，例如：Mycat-Server，shardingsphere等，MyProxy
的功能比较单一，但是针对加密字段等场景，能够提供更好的SQL语句的兼容性；例如：支持多表关联，INSERT INTO ... SELECT ...
等复杂SQL语句。

## 特性 
 * 采用全异步架构，单机能够支持上万个数据库连接，同时保证毫秒级延迟
 * 基于Netty的零拷贝技术以及流式编程模型，实现大结果集的低延迟低内存开销传输
 * 支持普通字段和加密字段之间的双向迁移
 * 支持垂直分表功能，同一个主表能够同时支持多个扩展表

## 使用教程
### 配置
具体细节参考 [myproxy.properties](https://github.com/James-xie/myproxy/blob/master/src/main/resources/myproxy.properties) 中的介绍

### 打包
```shell
./gradlew fatJar
```

### 启动
```shell
java -Dmyproxy.properties.location=[location of myproxy.properties] -jar [location of jar package] 
```

### 连接
```shell
mysql -h[proxy host] -u[user] -p[password] --port=[port]   --default-auth=mysql_native_password --default-character-set=utf8 
```



