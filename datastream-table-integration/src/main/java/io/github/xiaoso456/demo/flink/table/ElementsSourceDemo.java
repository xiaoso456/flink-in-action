package io.github.xiaoso456.demo.flink.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

public class ElementsSourceDemo {
    public static void main(String[] args) throws Exception {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



        DataStreamSource<String> dataStreamSource = env.fromElements("spark", "flink", "flink", "hive");
        // dataStream转化为table
        Table inputTable = tableEnv.fromDataStream(dataStreamSource);




        env.execute("datagen print job");
    }
}
