package com.atguigu.flinktutorial0815.chapter11;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class StreamToTableTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取数据流
        DataStreamSource<ClickEvent> stream1 = env.addSource(new ClickEventSource());
        Table table1 = tableEnv.fromDataStream(stream1, $("url"), $("user").as("uname"));
        table1.printSchema();

        DataStreamSource<Tuple2<Integer, String>> stream2 = env.fromElements(
                Tuple2.of(1, "a"),
                Tuple2.of(2, "b")
        );

        // 如果是元组类型，字段名默认就是f0、f1...
        Table table2 = tableEnv.fromDataStream(stream2, $("f1").as("myString"), $("f0").as("myInt"));

        table2.printSchema();

        // 如果是基本数据类型
        DataStreamSource<Integer> stream3 = env.fromElements(1, 2, 3, 4, 5);
        Table table3 = tableEnv.fromDataStream(stream3, $("myInt"));
        table3.printSchema();

        // 直接将流转换成表环境中注册的表
        tableEnv.createTemporaryView("clicks", stream1);

        // 从更新日志流读取数据，直接转换成有更新操作的表
//        tableEnv.fromChangelogStream()

        stream2.print();
        env.execute();
    }
}
