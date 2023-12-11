package io.github.xiaoso456.demo.flink.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        DataGeneratorSource<Long> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                return 1L;
            }
        }, 100, RateLimiterStrategy.perSecond(1), Types.LONG);


        WindowedStream<Long, Long, GlobalWindow> window = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-gen")
                .keyBy(new KeySelector<Long, Long>() {
                    @Override
                    public Long getKey(Long value) throws Exception {
                        return value;
                    }
                })
                // 这里使用计数窗口，窗口大小为5，滑动间隔为2
                .countWindow(5,2)
                ;

        // reduce对窗口进行增量聚合。增量聚合会在新数据到来时触发
        window.reduce(new ReduceFunction<Long>() {
            @Override
            public Long reduce(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }).print();


        env.execute("window demo");


    }
}
