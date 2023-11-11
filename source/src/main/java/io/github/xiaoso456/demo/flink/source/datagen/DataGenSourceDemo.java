package io.github.xiaoso456.demo.flink.source.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenSourceDemo {
    public static void main(String[] args) throws Exception {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(2);
        // 创建一个数据生成数据源，最大产生数据量为10,限制策略为每秒1条,返回类型为 string
        // 并行度大于1时，所有并行度的数据总量为10，限制策略为每个并行每秒1条
        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                // 根据传入的id生成数据
                return "input:" + aLong;
            }
        }, 10, RateLimiterStrategy.perSecond(1), Types.STRING);

        env.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "datagen")
                .print();

        env.execute("datagen print job");


    }
}
