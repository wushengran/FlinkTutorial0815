package com.atguigu.flinktutorial0815.chapter11;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
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

public class SimpleTableAndSQLTest {
    public static void main(String[] args) throws Exception{
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 创建一个流式表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 读取流式数据
        DataStreamSource<ClickEvent> clickStream = env.addSource(new ClickEventSource());

        // 2. 将流转换成表
        Table clickTable = tableEnv.fromDataStream(clickStream);

        // 3. 转换计算
        // 3.1 调用 Table API
        Table aliceTable = clickTable.select($("url"), $("user").as("uname"), $("ts"))
                .where($("uname").isEqual("Alice"));

        // 3.2 SQL
        Table bobTable = tableEnv.sqlQuery("select user as uname, url, ts from " + clickTable + " where user = 'Bob'");

        // 4. 转换成流输出
        tableEnv.toDataStream(aliceTable).print("alice");
        tableEnv.toDataStream(bobTable).print("bob");

        env.execute();
    }
}
