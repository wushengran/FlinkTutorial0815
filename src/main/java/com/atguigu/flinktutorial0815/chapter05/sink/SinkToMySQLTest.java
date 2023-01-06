package com.atguigu.flinktutorial0815.chapter05.sink;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class SinkToMySQLTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<ClickEvent> input = env.addSource(new ClickEventSource());

        input.addSink(JdbcSink.sink(
                "insert into clicks (user, url) values (?, ?)",
//                new JdbcStatementBuilder<ClickEvent>() {
//                    @Override
//                    public void accept(PreparedStatement preparedStatement, ClickEvent event) throws SQLException {
//                        preparedStatement.setString(1, event.user);
//                        preparedStatement.setString(2, event.url + ", " + event.ts);
//                    }
//                },
                (preparedStatement, event) -> {
                    preparedStatement.setString(1, event.user);
                    preparedStatement.setString(2, event.url + ", " + event.ts);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        env.execute();
    }
}
