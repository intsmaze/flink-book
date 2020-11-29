# 1.《深入理解Flink核心设计与实践原理》-随书代码
* **针对Flink的DataStream API,DataSet API,Table API进行了全面的讲解，相关特性均有完整的代码供读者运行和测试。**
* **整个工程共有【182个Java文件】，你要的Demo这里都有。**
* **本书是以Flink技术框架来讲解流计算这一技术领域的，但是流计算领域开发所面临的各种问题同样是Java后端开发者在进行服务端开发时所要面临的，比如有状态计算、Exactly Once语义等。因此Flink框架为解决这些问题而设计的方案同样值得Jave后端开发者借鉴。**
### 《深入理解Flink核心设计与实践原理》一书中针对该工程中的Flink示例代码进行了大量的详细讲解，为此推荐大家购买《深入理解Flink核心设计与实践原理》一书来获得更好的学习体验。

## 1.1 书籍目录 [点击查看详细目录](https://github.com/intsmaze/flink-book/blob/master/%E7%9B%AE%E5%BD%95.md)
本书针对Flink如下特性进行了详细的代码演示。
### 第1章　Apache Flink介绍
* ......
### 第2章　Apache Flink的安装与部署
* ......
### 第3章　Apache Flink的基础概念和通用API	34
* ......
### 第4章　流处理基础操作	58
* 4.1　DataStream的基本概念	58
* 4.2　数据流基本操作	70
* 4.3　富函数	89
* 4.4　任务链和资源组	92
* 4.5　物理分区	102
* 4.6　流处理的本地测试	121
* 4.7　分布式缓存	125
* 4.8　将参数传递给函数	133
### 第5章　流处理中的状态和容错	140
* 6.1　窗口 198
* 6.2　时间 239
* 6.3　数据流的连接操作	255
* 6.4　侧端输出	267
* 6.5　ProcessFunction	273
* 6.6　自定义数据源函数	279
* 6.7　自定义数据接收器函数	287
* 6.8　数据流连接器	290
### 第7章　批处理基础操作	320
* 7.1　DataSet的基本概念	320
* 7.2　数据集的基本操作	328
* 7.3　将参数传递给函数	344
* 7.4　广播变量	346
* 7.5　物理分区	349
* 7.6　批处理的本地测试	355
### 第8章　Table API和SQL	357
* 8.1　基础概念和通用API	357
* 8.2　SQL	374
* 8.3　Table API	387
* 8.4　自定义函数	388
* 8.5　SQL客户端	396
### 第9章　流处理中的Table API和SQL	410
* 9.1　动态表	410
* 9.2　时间属性	418
* 9.3　动态表的Join	423
* 9.4　时态表	429
* 9.5　查询配置	435
* 9.6　连接外部系统	436
### 第10章　执行管理	452
* ......


# 2. 技术咨询
## 2.1 购书读者免费进群
**购买本书的读者可以免费加入作者的QQ群，可以获得作者分享的最新技术文章，该群会不定时更新flink新版本中新特性的演示代码和内容讲解。**
QQ群号：941266442
  
书籍目录 [点击查看详细目录](https://www.cnblogs.com/intsmaze/)
![image](https://github.com/intsmaze/flink-book/blob/master/QQ.jpg)

# 3. [懒松鼠Flink-Boot脚手架工程](https://github.com/intsmaze/flink-book/blob/master/%E7%9B%AE%E5%BD%95.md)
1. 该脚手架工程集成Spring框架进行Bean管理，同时对Flink非转换操作代码进行封装，屏蔽掉Flink的API组装细节，使得普通开发者可以面向JAVA编程，快速编写业务代码。
2. **使用该脚手架工程可以满足一个项目组仅需一名开发人员具备Flink的基本知识，其他人员甚至不需要懂Flink，整个项目组都可以使用Flink框架解决业务中的痛点问题。**
3. **[关于该脚手架工程详情移步。](https://github.com/intsmaze/flink-book/blob/master/%E7%9B%AE%E5%BD%95.md)**


## 3.1 除此之外针对目前流行的各大Java框架，懒松鼠Flink-Boot脚手架工程也进行了集成，加快开发人员的编码速度。
* 集成Jbcp-template对Mysql,Oracle,SQLServer等关系型数据库的快速访问。
* 集成Hibernate Validator框架进行参数校验。
* 集成Spring Retry框架进行重试标志。
* 集成Mybatis框架,提高对关系型数据库增，删，改，查的开发速度。
* 集成Spring Cache框架,实现注解式定义方法缓存。
* ......

## 3.2 技术选项和集成情况
技术 | 名称 | 状态 | 免费版 | 会员版 |
----|------|----|------|----
Spring Framework | 容器  | 已集成 | 有  | 有
Spring 基于XML方式配置Bean | 装配Bean  | 已集成 | 有  | 有
Spring 基于注解方式配置Bean | 装配Bean  | 已集成 | 无  | 有
Spring 基于注解声明方法重试机制 | Retry注解  | 已集成 | 无  | 有
Spring 基于注解声明方法缓存 | Cache注解  | 已集成 | 无  | 有
Hibernate Validator | 校验框架  | 已集成 | 无  | 有
Druid | 数据库连接池  | 已集成 | 有  | 有
MyBatis | ORM框架  | 已集成 | 无  | 有
Mybatis-Plus | MyBatis扩展包  | 进行中 | 无  | 有
PageHelper | MyBatis物理分页插件  | 进行中 | 无  | 有
ZooKeeper | 分布式协调服务  | 进行中 | 无  | 有
Dubbo | 分布式服务框架  | 进行中 | 无  | 有
Redis | 分布式缓存数据库  | 进行中 | 有  | 有
Solr & Elasticsearch | 分布式全文搜索引擎  | 进行中 | 有  | 有
Ehcache | 进程内缓存框架  | 已集成 | 无  | 有
Kafka | 消息队列  | 已集成 | 有  | 有
HDFS | 分布式文件系统  | 已集成 | 有  | 有
Log4J | 日志组件  | 已集成 | 有  | 有
Junit | 单元测试  | 已集成 | 有  | 有
sequence | 分布式高效ID生产  | 进行中 | 有  | 有
Protobuf & json | 数据序列化  | 进行中 | 有  | 有
Dubbole消费者 | 服务消费者  | 进行中 | 有  | 有
Spring eurake消费者 | 服务消费者  | 进行中 | 有  | 有
Apollo配置中心 | 携程阿波罗配置中心  | 进行中 | 无  | 有
Spring Config配置中心 | Spring Cloud Config配置中心  | 进行中 | 无  | 有

