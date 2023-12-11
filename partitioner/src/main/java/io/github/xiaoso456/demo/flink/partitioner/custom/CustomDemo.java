package io.github.xiaoso456.demo.flink.partitioner.custom;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomDemo {
    public static void main(String[] args) throws Exception {


        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // 创建一个数据生成数据源，最大产生数据量为10,限制策略为每秒1条,返回类型为 string
        // 并行度大于1时，所有并行度的数据总量为10，限制策略为每个并行每秒1条
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                // 根据id 生成数据，划分为2种数据
                if (aLong % 2 == 0) {
                    return "user:0:" + aLong;
                }
                return "user:1:" + aLong;

            }
        }, 20, RateLimiterStrategy.perSecond(Long.MAX_VALUE), Types.STRING);


        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "datagen")
                .partitionCustom(new Partitioner<Long>() {
                    @Override
                    public int partition(Long key, int numPartitions) {
                        // 2. 由 key决定分区
                        // 上面key为userid，这种方式可以把同一个用户数据分配到同一个分区
                        return key.intValue() % 3;
                    }
                }, new KeySelector<String, Long>() {
                    @Override
                    public Long getKey(String value) throws Exception {
                        // 1.输入 user:userId:id，由KeySelector提取用于分区的Key
                        if (value.startsWith("user:")) {
                            return Long.parseLong(value.split(":")[1]);
                        }
                        return 3L;
                    }
                })
                .print();

        env.execute("CustomDemo print job");



    }


}
