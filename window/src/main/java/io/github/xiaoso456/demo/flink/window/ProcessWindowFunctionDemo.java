package io.github.xiaoso456.demo.flink.window;

import cn.hutool.core.date.DateUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

public class ProcessWindowFunctionDemo {
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

        // ProcessWindowFunction<IN, OUT, KEY, W extends Window>
        // 输入类型，输出类型，key类型，窗口类型
        window.process(new ProcessWindowFunction<Long, String, Long, TimeWindow>() {
                    /**
                     *
                     * @param aLong The key for which this window is evaluated.
                     * @param context The context in which the window is being evaluated.
                     * @param elements The elements in the window being evaluated.
                     * @param out A collector for emitting elements.
                     * @throws Exception
                     */
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<Long, String, Long, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                        TimeWindow curWindow = context.window();
                        long start = curWindow.getStart();
                        long end = curWindow.getEnd();
                        StringBuilder resultBuilder = new StringBuilder();
                        for (Long element : elements) {
                            resultBuilder.append(element).append(",");
                        }
                        resultBuilder.deleteCharAt(resultBuilder.length() - 1);

                        String result = resultBuilder.toString();
                        System.out.printf("窗口的起始时间为%s 结束时间为%s 计算结果为 %s\n", DateUtil.date(start), DateUtil.date(end),result);
                        out.collect(result);


                    }
                })
                .print();


        env.execute("window demo");


    }
}
