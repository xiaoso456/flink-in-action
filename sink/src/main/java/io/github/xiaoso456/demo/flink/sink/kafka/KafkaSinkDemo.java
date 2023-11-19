package io.github.xiaoso456.demo.flink.sink.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                // 根据传入的id生成数据
                return "input:" + aLong;
            }
        }, 2000, RateLimiterStrategy.perSecond(100), Types.STRING);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.229.128:9094")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("demo.flink")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 精确一次，保证数据不丢失
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 精确一次，设置事务id前缀
                .setTransactionalIdPrefix("demo-flink-transaction-")
                // 精确一次，需要设置事务超时时间，大于checkpoint的间隔，小于max
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(10 * 60 * 1000))
                .build();
        env
                .fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dategen")
                .sinkTo(kafkaSink)
                ;
        env.execute("kafka sink");

    }
}
