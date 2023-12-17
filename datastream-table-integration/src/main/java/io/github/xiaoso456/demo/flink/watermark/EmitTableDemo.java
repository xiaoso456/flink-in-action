package io.github.xiaoso456.demo.flink.watermark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class EmitTableDemo {
    public static void main(String[] args) {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 使用 table api创建一个表结构
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .build();
        // 创建临时表user_table，从本地文件读入数据
        tableEnv.createTemporaryTable("user_table", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "E:\\code\\idea_project\\new\\flink-in-action\\datastream-table-integration\\src\\main\\resources\\user.csv")
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", ",")
                        .build()
                )
                .build()
        );
        // 查询
        Table userTable = tableEnv.sqlQuery("select id,name,age from user_table");

        // 使用 sql api 创建临时表
        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'print') LIKE user_table (EXCLUDING ALL) ");

        // 把表输入到目标表
        TablePipeline sinkTable = userTable.insertInto("SinkTable");
        sinkTable.execute();

    }
}
