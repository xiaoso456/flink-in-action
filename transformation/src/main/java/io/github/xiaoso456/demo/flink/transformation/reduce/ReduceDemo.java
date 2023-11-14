package io.github.xiaoso456.demo.flink.transformation.reduce;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        // 创建一个数据生成数据源，最大产生数据量为10,限制策略为每秒1条,返回类型为 string
        // 并行度大于1时，所有并行度的数据总量为10，限制策略为每个并行每秒1条
        DataGeneratorSource<Long> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                // 根据传入的id生成数据，偶数加100
                if (aLong % 2 == 0) {
                    return aLong + 100;
                }
                return aLong;
            }
        }, 100, RateLimiterStrategy.perSecond(1), Types.LONG);

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "datagen")
                .keyBy(new KeySelector<Long, Long>() {
                    @Override
                    public Long getKey(Long value) throws Exception {
                        // 根据数据分区，区分偶数和奇数
                        return value % 2;
                    }
                })
                // 对不同分区求和
                .reduce(new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long value1, Long value2) throws Exception {
                        return value1 + value2;
                    }
                })
                .print();

        env.execute("keyByDemo job");


    }
}
