## 简介

项目随缘维护一些简单的 Flink Demo，版本主要是用 JDK11 + Flink1.18

## 项目

一个文件夹为一个可独立运行的Demo项目

### word-count

最基本的 Flink Demo，是用DataStream级别API实现，示例如何进行单词统计

基于socket无界流：

1. 启动 SocketApp，创建一个Socket服务端
2. 启动WordCountFromSocketApp，连接创建的Socket数据源
3. 在WordCountFromSocketApp控制台输入单词，WordCountApp会进行统计，并打印

基于file有界流，可以使用批处理方式

1. 启动WordCountFromFileApp

### source

演示了一些数据源的使用

#### kafka

使用示例见 KafkaSourceDemo

#### dataGen

dataGen是可以根据编码规则产生数据的数据源，支持限速

使用示例见 DataGenSourceDemo

#### Element

输入指定的元素

使用示例见 ElementsDemo

### transformation

演示一些算子使用

#### KeyBy

*DataStream -> KeyedStream*

KeyBy 会将 key 值相同的记录分配到相同的分区中，其作用类似 SQL 中的 group by。

在内部，KeyBy 是通过哈希分区实现的

使用示例见 KeybyDemo，示例中根据偶数奇数进行分区，并进行分区求和

#### Reduce

*KeyedStream -> DataStream*

Reduce 会将数据流中的元素与上一个 Reduce 后的元素进行合并且产生一个新值。这个合并发生在每次数据流入时，即每流入一个元素，都会有一次合并操作。

使用示例见 ReduceDemo，示例中根据偶数奇数进行分区，并进行分区求和

### partitioner

*DataStream -> DataStream*

partitioner对数据流提供了分区控制。其本质上，是将上游 Subtask 处理后的数据通过指定的分区策略输出到下游的 Subtask。

#### custom

自定义分区策略

使用示例见 CustomDemo，该例子自定义了一个分区策略

### sink

演示了一些输出源的使用

#### file sink

使用示例见 FileSinkDemo

### window

window分为时间创建和计数窗口

再按分配数据规则分类又可以分为：

1. 滚动窗口（Tumbiling Windows）

   窗口大小固定，窗口之间不重叠

2. 滑动窗口（Sliding Windows）

   窗口大小固定，窗口之间步长固定，窗口之间可重叠

3. 会话窗口（Session Windows）

   会话窗口之间不会重叠，长度不固定

4. 全局窗口（Global Windows）

   窗口一直有效，需要触发器触发计算

窗口计算方式一般有：

+ 增量聚合

  来一条数据计算一次

+ 全窗口函数

  数据只存起来，窗口触发时才计算



#### Count Windows

示例见 CountWindowDemo，该示例每秒输入一个value为1的数据，创建一个大小为5，间隔为2的计数窗口，并使用reduce对窗口进行增量聚合，每间隔2个数据输出输入数据总和

#### Tumbiling Windows

示例见 TumblingWindowDemo，该示例每秒输入一个value为1的数据，创建一个5s的滚动窗口，并使用reduce对窗口进行增量聚合，输出5s内输入数据总和。

#### Sliding Windows

示例见 SlidingWindowDemo，该示例每秒输入一个value为1的数据，创建一个窗口长度10s，滑动步长为5s滑动窗口，并使用reduce对窗口进行增量聚合，每5s输出10s内输入数据总和。

#### Aggregate函数

Aggregate函数适用于增量聚合时，需要自定义输入类型，累加器类型，输出类型的场合

示例见AggregateDemo，该示例每秒输入一个value为1的数据，创建一个5s的滚动窗口，使用aggregate进行聚合，输入类型为Long，累加器类型为String，输出类型为String。每来一次数据时调用一次add方法，每5s调用一次result方法输出结果

示例2见AggregateAndProcessWindowDemo，演示了aggregate+processWindow全窗口，聚合结果作为全窗口输入

#### 全窗口函数ProcessWindowFunction
### 时间和水位线

flink中有以下时间：

+ 事件时间（Event Time）
+ 摄取时间（Ingestion Time）
+ Processing Time（执行算子的事件）

水位线（watermark）：用来衡量事件时间的标记，可以看作是插入到数据中一条时间戳。水位线一般每隔一段时间才会生成一个，水位线是不断增长的，如果乱序数据时间比水位线慢，则不生成新的水位线，直到数据超过水位线。

全窗口用于触发计算时，把存起来的数据进行一次性计算，给出结果

示例见 ProcessWindowFunctionDemo，该示例每秒输入一个value为1的数据，创建一个5s的滚动窗口，使用自定义函数进行全窗口计算，每五秒对输入数据进行一次计算，使用`,`对5s内数据进行拼接
水位线常用于窗口计算，对于迟到数据，如果要纳入窗口计算，可以设置延迟，等上一定时间再生成到水位线。

水位线生成策略有两种，一种是基于现实时间驱动的，默认每200ms生成一个watermark，需要实现AssignerWithPeriodicWatermarks;另一种是基于特殊记录的，需要实现AssignerWithPunctuatedWatermarks

## CDC项目
Flink CDC项目是基于Flink实现的Change Data Capture（变更数据捕获）项目，主要用于实时捕获数据源的变更，并将变更数据实时写入到目标系统中。


### mysql-cdc

开启binlog，修改my.cnf

```
log-bin=/bitnami/mysql/mysql-bin
binlog_format=ROW
expire_logs_days=7
```

连接mysql设置时区

```sql
SET time_zone = 'Asia/Shanghai';
SET @@global.time_zone = 'Asia/Shanghai';
```

如果报错，说明没有时区文件，可以这么设置或者去官网下载时区文件

```sql
SET time_zone = '+08:00';
SET @@global.time_zone = '+08:00';
```



## 参考

[Flink 快速入门 | Panda Home (magicpenta.github.io)](https://magicpenta.github.io/docs/flink/Flink 快速入门/)

https://www.bilibili.com/video/BV1eg4y1V7AN

[Flink基础系列25-时间语义和Watermark - 知乎 (zhihu.com)](https://zhuanlan.zhihu.com/p/455305818)

[配置MySQL-阿里云帮助中心_(Flink)-阿里云帮助中心 (aliyun.com)](https://help.aliyun.com/zh/flink/configure-a-mysql-database?spm=a2c4g.11186623.0.0.752a7c6dalaR3M#concept-2116236)
