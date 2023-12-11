package io.github.xiaoso456.demo.flink.source.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.229.128:9094")
                .setGroupId("flink-consumer")
                .setTopics("test")
                // 初始化偏移量为最后
                .setStartingOffsets(OffsetsInitializer.latest())
                // 设置value序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka source")
                .print();

        env.execute("kafka print job");


    }
}
