package com.atguigu.flinktutorial0815.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class TableToDataStreamTest {
    public static void main(String[] args) throws Exception{
        // 如果要做流表转换，必须要用流式表环境，需要基于流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建连接器表
        tableEnv.executeSql("create table clicks (" +
                " uname varchar(20)," +
                " url string," +
                " ts bigint" +
                ") with (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")");

        // 对表进行查询转换
        Table bobTable = tableEnv.sqlQuery("select url, uname, ts from clicks where uname = 'Bob'");
        Table userCountTable = tableEnv.sqlQuery("select uname, count(*) as cnt from clicks group by uname");

        tableEnv.toDataStream(bobTable).print("bob");
        tableEnv.toChangelogStream(userCountTable).print("user_count");

        env.execute();
    }
}
