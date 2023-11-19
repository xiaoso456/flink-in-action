package io.github.xiaoso456.demo.flink.source.elements;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ElementsSourceDemo {
    public static void main(String[] args) throws Exception {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> dataStreamSource = env.fromElements("spark", "flink", "flink", "hive");
        dataStreamSource.print();


        env.execute("datagen print job");
    }
}
