package io.github.xiaoso456.demo.flink.cdc.mysql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

@SpringBootApplication
@MapperScan("io.github.xiaoso456.demo.flink.cdc.mysql.mapper")
public class App {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(App.class, args);
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(10000L);
        env.setStateBackend(new FsStateBackend("file:///code/tmp/flink-cdc-state-table"));
        env.setParallelism(4);

        String sourceEventDDL = FileUtils.readFile(new File("D:\\code\\project\\idea_project\\flink-in-action\\cdc\\src\\main\\resources\\source_event.sql"), "utf-8");
        tEnv.executeSql(sourceEventDDL);
        String sourceAssetDDL = FileUtils.readFile(new File("D:\\code\\project\\idea_project\\flink-in-action\\cdc\\src\\main\\resources\\source_asset.sql"), "utf-8");
        tEnv.executeSql(sourceAssetDDL);
        String sourceEventAssetLinkDDL = FileUtils.readFile(new File("D:\\code\\project\\idea_project\\flink-in-action\\cdc\\src\\main\\resources\\source_asset_event.sql"), "utf-8");
        tEnv.executeSql(sourceEventAssetLinkDDL);

        String sinkDDL = FileUtils.readFile(new File("D:\\code\\project\\idea_project\\flink-in-action\\cdc\\src\\main\\resources\\sink-jdbc.sql"), "utf-8");
        tEnv.executeSql(sinkDDL);


        String insertSql = FileUtils.readFile(new File("D:\\code\\project\\idea_project\\flink-in-action\\cdc\\src\\main\\resources\\insert.sql"), "utf-8");
        tEnv.executeSql(insertSql);


        env.execute("mysql cdc table demo");



    }
}
