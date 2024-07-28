package io.github.xiaoso456.demo.flink.cdc.mysql;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class MysqlCDCDemo {
    public static void main(String[] args) throws Exception {

        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.enableCheckpointing(10000L);
        env.setStateBackend(new FsStateBackend("file:///code/tmp/flink-cdc-state",true));

        // 初始化 table env
        Properties properties = new Properties();
        properties.setProperty("time-zone", "+08:00");
        // 初始化mysql source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.229.128")
                .port(32710)
                .jdbcProperties(properties)
                .serverTimeZone("+08:00")
                .username("root")
                .password("jpKSovB6mB")
                .databaseList("my_database")
                .tableList("my_database.user1")
                // .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql source")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("mysql cdc demo");


    }
}
