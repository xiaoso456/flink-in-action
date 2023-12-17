package io.github.xiaoso456.demo.flink.window;

import cn.hutool.core.date.DateUtil;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AggregateAndProcessDemo {
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


        WindowedStream<Long, Long, TimeWindow> window = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-gen")
                .keyBy(new KeySelector<Long, Long>() {
                    @Override
                    public Long getKey(Long value) throws Exception {
                        return value;
                    }
                })
                // 这里使用滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // aggregate对窗口进行增量聚合
        // aggregate(AggregateFunction<IN, ACC, OUT>) 输入类型IN，累加器类型ACC，输出类型OUT
        AggregateFunction<Long, String, String> aggregateFunction = new AggregateFunction<Long, String, String>() {
            @Override
            public String createAccumulator() {
                System.out.println("创建累加器");
                return "";
            }

            @Override
            public String add(Long aLong, String acc) {
                System.out.println("调用add方法");
                return acc + " " + String.valueOf(aLong);
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
        };

        // IN, OUT, KEY, W extends Window
        ProcessWindowFunction<String, String, Long, TimeWindow> processWindowFunction = new ProcessWindowFunction<String, String, Long, TimeWindow>() {
            @Override
            public void process(Long key, ProcessWindowFunction<String, String, Long, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                TimeWindow curWindow = context.window();
                long start = curWindow.getStart();
                long end = curWindow.getEnd();
                StringBuilder resultBuilder = new StringBuilder();
                for (String element : elements) {
                    resultBuilder.append(element).append(",");
                }
                resultBuilder.deleteCharAt(resultBuilder.length() - 1);

                String result = resultBuilder.toString();
                System.out.printf("窗口的起始时间为%s 结束时间为%s 计算结果为 %s\n", DateUtil.date(start), DateUtil.date(end),result);
                out.collect(result);
            }
        };

        window.aggregate(aggregateFunction,processWindowFunction).print();


        env.execute("window demo");


    }
}
