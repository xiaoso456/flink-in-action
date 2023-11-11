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

见到 KafkaSourceDemo

#### dataGen



## 参考

[Flink 快速入门 | Panda Home (magicpenta.github.io)](https://magicpenta.github.io/docs/flink/Flink 快速入门/)

