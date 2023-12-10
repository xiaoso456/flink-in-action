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

partitioner是分区策略

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

示例见AggregateDemo，该示例该示例每秒输入一个value为1的数据，创建一个5s的滚动窗口，使用aggregate进行聚合，输入类型为Long，累加器类型为String，输出类型为String。每来一次数据时调用一次add方法，每5s调用一次result方法输出结果



## 参考

[Flink 快速入门 | Panda Home (magicpenta.github.io)](https://magicpenta.github.io/docs/flink/Flink 快速入门/)

https://www.bilibili.com/video/BV1eg4y1V7AN
