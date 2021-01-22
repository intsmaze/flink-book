# 1.《深入理解Flink核心设计与实践原理》-随书代码
* **整个工程共有【182个Java文件】，你要的Demo这里都有。**
### 《深入理解Flink核心设计与实践原理》一书中针对该工程中的Flink示例代码进行了大量的详细讲解，为此推荐大家购买《深入理解Flink核心设计与实践原理》一书来获得更好的学习体验。
## [《深入理解Flink核心设计与实践原理》京东商城购买链接](https://item.jd.com/12765369.html)
![image](https://github.com/intsmaze/flink-book/blob/master/fm.png)

* 本书从Apache Flink的缘起开始，由浅入深，理论结合实践，全方位地介绍Apache Flink这一处理海量数据集的高性能工具。本书围绕部署、流处理、批处理、Table API和SQL四大模块进行讲解，并详细说明Apache Flink的每个特性的实际业务背景，使读者不仅能编写可运行的Apache Flink程序代码，还能深刻理解并正确地将其运用到合适的生产业务环境中。
* 虽然本书是以Apache Flink技术框架来讲解流计算技术的，但是流计算领域开发所面临的各种问题同样是Java后端开发者在进行服务端开发时所要面临的，如有状态计算、Exactly Once语义等。因此，Apache Flink框架为解决这些问题而设计的方案同样值得Java后端开发者借鉴。


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
  
QQ群号：941266442 [群二维码无法显示可跳转该页面扫码](https://www.cnblogs.com/intsmaze/)
![image](https://github.com/intsmaze/flink-book/blob/master/QQ.jpg)


# Flink 1.8版本公告
## 新特点与改进
* 最终状态模式演变故事：Flink 1.8版本为Flink管理的用户状态提供了模式演变故事。从引入了对Avro状态模式演化的支持以及改进的序列化兼容性抽象的Flink 1.7版本开始，这一工作已经跨越了2个版本。Flink 1.8版本通过将模式演化支持扩展到pojo，升级所有Flink内置序列化器以使用新的序列化兼容性抽象，以及使使用自定义状态序列化器的高级用户更容易实现这些抽象。

* 基于TTL的旧状态的持续清理:在Flink 1.6版本中引入了用于Keyed状态的生存时间TTL(Time-to-Live)(Flink-9510)。如果状态配置了生存时间，并且状态的值已过期，Flink将尽最大努力清理存储的过期状态值，同时在写入保存点/检查点时也将清除状态。在Flink 1.8版本中引入了对RocksDB状态后端和堆状态后端的过期状态的持续清理。这意味着过期状态(根据TTL设置)将不断被清理，不在需要用户通过访问过期状态来触发过期值的清理。

* 具有用户定义函数和聚合的SQL模式检测：通过多个特性扩展了对 MATCH_RECOGNIZE子句的支持。用户定义函数的添加允许在模式检测期间自定义逻辑（FLINK-10597），而添加聚合则允许更复杂的CEP定义，例如以下内容（FLINK-7599）。
```
SELECT *
FROM Ticker
    MATCH_RECOGNIZE (
        ORDER BY rowtime
        MEASURES
            AVG(A.price) AS avgPrice
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO FIRST B
        PATTERN (A+ B)
        DEFINE
            A AS AVG(A.price) < 15
    ) MR;
```
* RFC-compliant CSV 格式：当前Flink版本中可以以符合RFC-4180标准的CSV表格式读写SQL表。

* 新的KafkaDeserializationSchema可以被用来直接访问ConsumerRecord：对于Flink 的KafkaConsumers，社区引入了可以直接访问Kafka ConsumerRecord的新KafkaDeserializationSchema。现在用户可以访问Kafka提供给记录的所有数据，包括头信息。这包含KeyedSerializationSchema的功能，该功能已过时，但现在仍可用。

* FlinkKinesisConsumer（FLINK-5697）中的每个分片水印选项：Kinesis Consumer现在可以发出来自每个分片水印的周期性水印，以便正确处理使用多个Kinesis分片的子任务的事件时间。

* 用于捕获表更改的DynamoDB流的新消费者（FLINK-4582）：FlinkDynamoDBStreamsConsumer是Kinesis消费者的一种变体，它支持从DynamoDB表中检索类似CDC的流。 

* 支持子任务协调的全局聚合（FLINK-10887）：GlobalAggregateManager被设计为用于全局源水印跟踪的解决方案，允许在并行子任务之间共享信息。该特性将被集成到用于水印同步的流连接器中，并可与用户定义的聚合器一起用于其他目的。

## 重要变化
* Flink（FLINK-11266）绑定Hadoop库的更改：不再发布包含Hadoop的便捷二进制文件。如果部署的Flink集群依赖于flink-dist中包含的flink-shaded-hadoop2，则必须从下载页面（https://flink.apache.org/downloads.html） 的附加组件部分中手动下载预先打包的Hadoop jar，并将其复制到<flink_home>/lib目录中，或者可以通过打包flink-dist并激活include-hadoop maven配置文件来构建包含hadoop的Flink发行版。

* FlinkKafkaConsumer现在将根据主题规范过滤还原的分区：从Flink 1.8版本开始，FlinkKafkaConsumer现在总是过滤掉在恢复执行中不再与要订阅的指定主题相关联的已恢复的分区。这种行为在以前版本的FlinkKafkaConsumer中不存在。如果您希望保留以前的行为，请在FlinkKafkaConsumer上调用disableFilterRestoredPartitionsWithSubscribedTopics()配置方法。

比如以行场景：如果有一个从主题A消费的Kafka消费者，则执行一个保存点，然后将这个Kafka消费者更改为从主题B消费，然后从该保存点重新开始工作。在进行此更改之前，您的消费者现在将同时从主题A和B中进行消费，因为存储状态是消费者正在从主题A中进行消费。通过更改，您的消费者将仅在还原后从主题B中进行消费，因为它现在过滤存储在状态的主题而是使用配置的主题。


* Table API的Maven模块中的更改：以前使用flink-table依赖项的用户需要将其依赖项更新为flink-table-planner以及flink-table-api-*的正确依赖关系，关于flink-table-api- *的选择取决于使用的是Java还是Scala：flink-table-api-java-bridge或flink-table-api-scala-bridge。


# Flink 1.9版本公共
###  细粒度的批量恢复
批处理（DataSet，Table API和SQL）作业从任务失败中恢复的时间将大大减少。在Flink 1.9版本之前，当批处理作业中的某个任务发生失败时通过取消批处理作业的所有任务并重新启动整个批处理作业来恢复批处理作业，即该作业从头开始启动，之前的所有进度都无效。在当前Flink版本中，可以将Flink配置为仅将恢复限制为位于相同故障转移区域的那些任务。故障转移区域是通过流水线数据交换连接的一组任务。因此，作业的批处理连接定义其故障转移区域的边界。更多细节可以在FLIP-1中找到。
![](index_files/b5fdd6ef-1013-4603-8848-68fcc9647682.jpg)

要使用新的故障转移策略，用户需要确保在flink-conf.yaml中具配置jobmanager.execution.failover-strategy：region。
注意：Flink 1.9版本的配置中默认情况下配置并启动了该策略，但是当重新使用先前Flink版本中的配置文件时，用户必须手动添加它。

此外，用户还需要在批处理作业中将ExecutionConfig的ExecutionMode设置为BATCH，以配置数据转移不是流水线化的，并且作业有多个故障转移区域。

###  状态处理器API
在Flink 1.9版本之前，从外部访问Flink应用程序中状态仅限于使用实验性地Queryable State API。在当前Flink版本中引入了一个新的功能强大的库，可以使用批处理DataSet API读取，写入和修改状态快照。实际上这意味着：
* 可以通过从外部系统（例如外部数据库）读取数据并将其转换为保存点来引导Flink作业状态。
* 可以识别并更正保存点中的无效数据。
* 与之前需要在线访问模式迁移的方法相比，保存点中的状态模式可以离线迁移。
* 可以使用Flink的任何批处理API（DataSet，Table，SQL）查询保存点中的状态.

新的状态处理器API涵盖了快照的所有变体：保存点，完整检查点和增量检查点。

###  带有保存点的停止
带有保存点的取消操作是停止/重新启动，更新Flink作业的常用操作。但是，现有的实现方式无法保证exactly-once语义的接收器对外部存储系统的输出持久性。为了改善停止作业时的端到端语义，Flink 1.9版本中引入了一种新SUSPEND模式，使用与发出的数据一致的保存点来停止作业。可以使用Flink的CLI客户端命令挂起作业，如下所示：
```
<flink_home>bin/flink stop -p [:targetDirectory] :jobId
```

### Flink WebUI 重构
当前Flink版本使用Angular的最新稳定版本重建了Flink WebUI,同时保存了一个链接可以切换到旧的WebUI。
![](index_files/81a2943d-aebb-4d79-a185-b55204fc1856.png)

注意：在未来的Flink发行版中将无法保证旧版本WebUI的功能与新版WebUI的功能均等

### 新的Blink SQL查询处理器预览
在Blink捐赠给Apache Flink之后，社区致力于为Table API和SQL集成Blink的查询优化器。Flink扩展了Blink的计划器，以实现新的优化器接口，以便现在有两个可插入的查询处理器来执行Table API和SQL语句：1.9版本之前的Flink处理器和新的基于Blink的查询处理器。
基于Blink的查询处理器提供了更好的SQL覆盖范围（1.9版本中对TPC-H进行了完整的覆盖范围，计划在下一个Flink版本中提供TPC-DS覆盖范围），通过更广泛的查询优化提高了批查询的性能，改进了代码生成，并调整了操作符实现。基于Blink的查询处理器还提供了更强大的流处理运行程序，具有一些新功能（例如维表连接，TopN，重复数据删除）和优化以解决聚合中的数据偏斜以及更有用的内置函数。

注意：这两种查询处理器的语义和支持的操作集大部分是一致的，但不完全一致。

当前Flink版本中对Blink的查询处理器的集成尚未完全完成。因此Flink 1.9版本之前的Flink处理器仍然是Flink 1.9版本中的默认处理器，并建议用户用于生产环境。

#### Table API和SQL的其他改进
* 适用于Java用户的无Scala Table API和SQL（FLIP-32）
作为重构和拆分flink-table模块的一部分，为Java和Scala创建了两个单独的API模块。对于Scala用户而言，什么都没有真正改变，但是Java用户现在可以使用Table API和或SQL，而无需获取Scala依赖项。

* 重构Table API类型系统（FLIP-37）
社区实现了一个新的数据类型系统，以将Table API与Flink的TypeInformation类分离，并提高其与SQL标准的兼容性。这项工作仍在进行中，预计将在下一个版本中完成。在Flink 1.9版本中，UDF以及其他功能还没有移植到新的类型系统中。

* Table API（FLIP-29）的多列和多行转换
通过一系列支持多行或多列输入和输出的转换，Flink扩展了Table API的功能。这些转换极大地简化了处理逻辑的实现，而用户使用关系运算符来实现这些逻辑是十分很麻烦。

* 新的统一Catalog  API（FLIP-30）
重新设计了Catalog API以存储元数据并统一了内部和外部目录的处理。这项工作主要是作为Hive集成的前提条件而发起的，但是也提高了在Flink中管理目录元数据的整体便利性。除了改进Catalog 接口之外，还扩展了它们的功能。以前Table API或SQL查询的表定义是可变的。使用Flink 1.9版本后可以将用SQL DDL语句注册的表的元数据保留在目录中，这意味着用户可以将由Kafka主题支持的表添加到Metastore目录中，然后在用户的目录连接到Metastore时查询该表。

* SQL API中的DDL支持（FLINK-10232）
到目前为止，Flink SQL仅支持DML语句（例如SELECT，INSERT）。外部表（表源和接收器）必须通过Java/Scala代码或配置文件进行注册。对于Flink 1.9版本，社区增加了对SQL DDL语句的支持，比如注册和删除表和视图（CREATE TABLE，DROP TABLE）。

* 完整Hive集成预览（FLINK-10556）
Apache Hive在Hadoop的生态系统中被广泛使用，以存储和查询大量的结构化数据。除了作为查询处理器之外，Hive还具有一个名为Metastore的目录，用于管理和组织大型数据集。查询处理器的常见集成点是与Hive的Metastore集成，以便能够利用Hive管理的数据。

社区开始为Flink的Table API和SQL实施连接到Hive的Metastore的外部目录。在Flink 1.9版本中，用户将能够查询和处理Hive中存储的所有数据。如前所述，用户还可以将Flink表的元数据保留在Metastore中。此外Hive集成还包括支持在Flink Table API或SQL查询中使用Hive的UDF。

请注意，Flink 1.9版本中的对Hive的支持是试验性的，社区计划在下一个版本中稳定这些功能。

## 重要变化
* Table API和SQL现在是Flink发行版默认配置的一部分。 以前必须通过将相应的JAR文件从<flink-home>/opt移到<flink-home>/lib来启用Table API和SQL特性。
* 为了准备FLIP-39，已删除了机器学习库（flink-ml）。
* 为了支持FLIP-38，已删除了旧的DataSet和DataStream Python API。
* Flink可以在Java 9上编译并运行。请注意与外部系统交互的某些组件（连接器，文件系统，报告器）可能无法工作，因为各个组件可能已跳过对Java 9支持.
  
  # Flink 1.10版本公告
  ## 新特点与改进
### 改进的内存管理和配置
之前Flink版本对TaskExecutor的内存配置存在一些缺陷，这些缺陷使得Flink难以优化资源利用率，例如：
* 流和批处理执行中不同的内存占用模型；
* 流处理执行中off-heap状态后端(即RocksDB)是十分复杂且过于依赖于用户的配置。

为了使内存的配置选项对用户更明确和直观，在Flink 1.10版本中对TaskExecutor内存模型和配置逻辑（FLIP-49）进行了重大更改。 这些更改使Flink更适合于各种部署环境（例如Kubernetes，Yarn，Mesos），从而使用户可以严格控制其内存消耗。

#### 托管内存扩展
Flink扩展了托管内存，以解决RocksDB状态后端在内存使用上的问题。虽然批处理作业可以使用on-heap或off-heap内存，但具有RocksDB状态后端的流处理作业只能使用off-heap内存。 为了允许用户在流处理执行和批处理执行之间随意切换而不必修改集群配置，托管内存现在始终处于off-heap状态。

#### 简化的RocksDB配置

在之前的Flink版本中配置RocksDB这样的off-heap状态后端需要用户进行大量的手动调整，例如减小JVM堆大小或将Flink设置为使用off-heap内存。在Flink 1.10版本中用户可以通过Flink的现成配置来实现，调整RocksDB状态后端的内存预算就像调整托管内存的大小一样简单。

### 提交作业的统一逻辑
在之前的Flink版本，提交作业是执行环境职责的一部分，并且与不同的部署方式（例如，Yarn，Kubernetes，Mesos）紧密相关，这导致用户需要单独配置和管理的定制环境越来越多。

在Flink 1.10版本中，作业提交逻辑被抽象到通用的Executor接口（FLIP-73）中。ExecutorCLI (FLIP-81)的添加引入了为任何执行目标指定配置参数的统一方法。为了完成这项工作，Flink引入了负责获取JobExecutionResult的JobClient (FLINK-74)，从而将结果检索过程与作业提交解耦。

通过为用户提供Flink的统一入口点，这些变化使得在下游框架(例如Apache Beam或Zeppelin interactive notebook)中以编程方式使用Flink变得更加容易。对于跨多个目标环境使用Flink的用户，转换到基于配置的执行过程还可以显著减少样板代码和可维护性开销

### Table API / SQL：与Hive集成
Flink与Hive集成在Flink 1.9版本中作为一个预览特性发布。这个特性允许用户使用SQL DDL在Hive Metastore中持久化与Flink相关的元数据(例如Kafka表)，调用在Hive中定义的UDF，并使用Flink来读写Hive表。在Flink 1.10版本中通过进一步的开发进一步完善了这项工作，这些开发可立即与生产的Hive集成，并与大多数Hive版本完全兼容。

#### 批处理SQL的原生分区支持 
之前的Flink版本，Flink SQL仅支持对未分区的Hive表进行写操作。在Flink 1.10版本中，通过INSERT OVERWRITE和PARTITION（FLIP-63）对Flink SQL语法进行了扩展，从而使用户能够对分区的Hive表进行写操作。

**静态分区写入**
```
INSERT { INTO | OVERWRITE } TABLE tablename1 [PARTITION (partcol1=val1, partcol2=val2 ...)] select_statement1 FROM from_statement;
```

**动态分区写入**
```
INSERT { INTO | OVERWRITE } TABLE tablename1 select_statement1 FROM from_statement;
```
分区表允许用户在读取表时利用分区剪裁特性以减少需要扫描的数据量，这可以显著提高读取操作的性能。

#### 进一步优化
除了分区剪裁外，Flink 1.10还为Hive的集成引入了更多读取优化方式，例如：

* 投影下推：Flink通过省略表扫描中不必要的字段，利用投影下推来最大程度地减少Flink和Hive表之间的数据传输。 这对于具有大量列的表尤其有利。

* LIMIT下推：对于带有LIMIT子句的查询，Flink将尽可能限制输出记录的数量，以最大程度地减少通过网络传输的数据量。

* 读取时的ORC向量化：为了提高ORC文件的读取性能，Flink现在默认将本机ORC向量化阅读器用于2.0.0以上的Hive版本以及具有非复杂数据类型的列。


### Table API / SQL的其他改进

#### SQL DDL中的水印和计算列
Flink 1.10版本支持特定于流处理的语法扩展，以在Flink的SQL DDL（FLIP-66）中定义时间属性和水印生成。这样用户就可以进行基于时间的操作（例如窗口），并且可以在使用DDL语句创建的表上定义水印策略。
```
CREATE TABLE table_name (
  WATERMARK FOR columnName AS <watermark_strategy_expression>
) WITH (
  ...
)
```
#### 完整的TPC-DS批处理范围
TPC-DS是一种广泛使用的行业标准决策支持基准，用于评估和衡量基于SQL的数据处理引擎的性能。在Flink 1.10版本中，所有TPC-DS查询均受端到端（FLINK-11491）支持。

## 重要改变
* [FLINK-10725] Flink现在可以编译并在Java 11上运行。

* [FLINK-15495] Blink计划程序现在是SQL Client中的默认设置，因此用户可以从所有最新功能和改进中受益。在下一版本中，还计划对Table API中的旧计划程序进行切换，因此建议用户开始熟悉Blink计划器。

* [FLINK-13025] 有一个新的Elasticsearchsink连接器，完全支持Elasticsearch 7.x版本。

* [FLINK-15115] Kafka 0.8和0.9的连接器已标记为已弃用，将不再得到积极支持。

* [FLINK-14516] 删除了基于非信用的网络流量控制代码，以及taskmanager.network.credit.model配置选项。以后Flink将始终使用基于信用的网络流量控制。

* [FLINK-11956]s3-hadoop和s3-presto 文件系统不再使用类重定位，而应通过插件加载。

* Flink 1.9版本中附带了一个重构的Web UI，同时保留了旧版的UI作为备份，以防事情无法按预期进行。到目前为止，尚无任何问题的报告，因此Flink社区投票决定将旧版的Web UI丢弃在Flink 1.10版本中。

* Flink不再支持使用.bat脚本启动群集。用户应改用WSL或Cygwin之类的环境，并使用.sh脚本启动集群。
