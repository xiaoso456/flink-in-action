package io.github.xiaoso456.demo.flink.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class AggregateDemo {
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
                .countWindow(5, 2);

        // aggregate对窗口进行增量聚合
        // aggregate(AggregateFunction<IN, ACC, OUT>) 输入类型IN，累加器类型ACC，输出类型OUT
        window.aggregate(new AggregateFunction<Long, String, String>() {
            @Override
            public String createAccumulator() {
                System.out.println("创建累加器");
                return "";
            }

            @Override
            public String add(Long aLong, String acc) {
                System.out.println("调用add方法");
                return acc +" "+ String.valueOf(aLong);
            }

            @Override
            public String getResult(String acc) {
                System.out.println("调用result方法，输出计算结果");
                return acc;
            }

            @Override
            public String merge(String acc1, String acc2) {
                // 该方法只有会话窗口会使用
                System.out.println("调用merge方法，合并中间结果");
                return acc1 + " " + acc2;
            }
        }).print();


        env.execute("window demo");


    }
}
