package io.github.xiaoso456.demo.flink.sink.file;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class FileSinkDemo {
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

        FileSink<String> stringFileSink = FileSink
                // 按行输出行式存储文件
                .<String>forRowFormat(new Path("D://tmp"), new SimpleStringEncoder<>("UTF-8"))
                .withOutputFileConfig(
                        OutputFileConfig
                                .builder()
                                .withPartPrefix("part-")
                                .withPartSuffix(".txt")
                                .build()
                )
                // 按目录分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd", ZoneId.systemDefault()))
                // 文件滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy
                                .builder()
                                // 1M文件大小滚动
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                // 或10s 滚动
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .build()
                )
                .build();
        // 开启 checkpoint
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        // 多少个并行度，多少个文件
        env.setParallelism(2);
        env
                .fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "datagen")
                .sinkTo(stringFileSink);
        env.execute("file sink job");

    }
}
