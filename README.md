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

## 参考

[Flink 快速入门 | Panda Home (magicpenta.github.io)](https://magicpenta.github.io/docs/flink/Flink 快速入门/)

