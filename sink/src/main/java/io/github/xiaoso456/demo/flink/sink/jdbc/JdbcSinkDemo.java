package io.github.xiaoso456.demo.flink.sink.jdbc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcSinkDemo {
    public static void main(String[] args) throws Exception {
        // 基于本地模式，开启 web ui
        Configuration configuration = new Configuration();
        // web ui 端口
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        SinkFunction<Department> jdbcSinkFuction = JdbcSink.sink(
                "insert into department values(?,?,?)",
                new JdbcStatementBuilder<Department>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Department department) throws SQLException {
                        // 占位符1 2 3 设置的值
                        preparedStatement.setLong(1, department.getId());
                        preparedStatement.setString(2, department.getName());
                        preparedStatement.setString(3, department.getDescription());
                    }
                },
                JdbcExecutionOptions.builder()
                        // 最大重试3次
                        .withMaxRetries(3)
                        // 批处理最大等待3000ms
                        .withBatchIntervalMs(3000)
                        // 一次提交2条数据
                        .withBatchSize(2)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/flink_test")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("admin")
                        // 连接超时时间
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()

        );
        env.fromElements(
                        new Department(1L, "研发部", "研发部门"),
                        new Department(2L, "市场部", "市场部门"),
                        new Department(3L, "财务部", "财务部门"))
                .addSink(jdbcSinkFuction);


        env.execute("jdbc sink");
    }
}
