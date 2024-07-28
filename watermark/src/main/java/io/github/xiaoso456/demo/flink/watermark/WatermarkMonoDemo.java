package io.github.xiaoso456.demo.flink.watermark;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedDeque;

public class WatermarkMonoDemo {
    public static void main(String[] args) throws Exception {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200L);


        // 定义 event 类如何取watermark
        // 使用了 forMonotonousTimestamps
        WatermarkStrategy<Event> eventWatermarkStrategy = WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getCreateTime();
                    }
                });

        // 定义事件输入
        ConcurrentLinkedDeque<Event> inputEvent = new ConcurrentLinkedDeque<>();
        inputEvent.offer(new Event(1L, "event", DateUtil.parse("2023-12-01 00:00:00").toTimestamp().getTime()));
        inputEvent.offer(new Event(2L, "event", DateUtil.parse("2023-12-01 00:00:03").toTimestamp().getTime()));
        inputEvent.offer(new Event(3L, "event", DateUtil.parse("2023-12-01 00:00:02").toTimestamp().getTime()));
        inputEvent.offer(new Event(4L, "event", DateUtil.parse("2023-12-01 00:00:10").toTimestamp().getTime()));

        inputEvent.offer(new Event(11L, "event", DateUtil.parse("2023-12-01 01:00:00").toTimestamp().getTime()));
        inputEvent.offer(new Event(12L, "event", DateUtil.parse("2023-12-01 01:00:06").toTimestamp().getTime()));
        // 由于13L这条已经是迟到数据，不允许进入窗口
        inputEvent.offer(new Event(13L, "event", DateUtil.parse("2023-12-01 01:00:02").toTimestamp().getTime()));
        inputEvent.offer(new Event(14L, "event", DateUtil.parse("2023-12-01 01:00:10").toTimestamp().getTime()));
        DataGeneratorSource<Event> eventDataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, Event>() {
            @Override
            public Event map(Long aLong) throws Exception {
                return inputEvent.poll();
            }
        }, inputEvent.size(), RateLimiterStrategy.perSecond(1), Types.POJO(Event.class));

        env.fromSource(eventDataGeneratorSource,eventWatermarkStrategy,"source")
                .keyBy(new KeySelector<Event, Integer>() {
                    @Override
                    public Integer getKey(Event value) throws Exception {
                        return 1;
                    }
                })
                // 滚动窗口，特别注意，这里要使用事件时间语义的滚动窗口TumblingEventTimeWindows
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 全窗口聚合
                .process(new ProcessWindowFunction<Event, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        TimeWindow curWindow = context.window();
                        long start = curWindow.getStart();
                        long end = curWindow.getEnd();
                        long curTime = System.currentTimeMillis();
                        StringBuilder resultBuilder = new StringBuilder();
                        for (Event element : elements) {
                            resultBuilder.append(element.getId()).append(",");
                        }
                        resultBuilder.deleteCharAt(resultBuilder.length() - 1);

                        String result = resultBuilder.toString();
                        System.out.printf("窗口的起始时间为%s 结束时间为%s 当前时间为%s  当前watermark为%s 计算结果为 %s\n"
                                , DateUtil.date(start), DateUtil.date(end),DateUtil.date(curTime),DateUtil.date(context.currentWatermark()),result);
                        out.collect(result);
                    }
                })
                .print();

        env.execute("event source job");


    }
}
