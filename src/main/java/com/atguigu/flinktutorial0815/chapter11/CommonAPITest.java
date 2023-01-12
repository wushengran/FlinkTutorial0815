package com.atguigu.flinktutorial0815.chapter11;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class CommonAPITest {
    public static void main(String[] args) {
        // 直接创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 1. 创建连接器表
        tableEnv.executeSql("create table clicks (" +
                " uname varchar(20)," +
                " url string," +
                " ts bigint" +
                ") with (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")");

        // 2. 查询转换
        Table bobTable = tableEnv.sqlQuery("select url, uname, ts from clicks where uname = 'Bob'");
        Table userCountTable = tableEnv.sqlQuery("select uname, count(*) as cnt from clicks group by uname");

        // 3. 创建输出表
        tableEnv.executeSql("create table output (" +
                " url string," +
                " uname varchar(20)," +
                " ts bigint" +
                ") with (" +
                " 'connector' = 'print' " +
                ")");
        tableEnv.executeSql("create table user_count (" +
                " uname varchar(20)," +
                " cnt bigint" +
                ") with (" +
                " 'connector' = 'print' " +
                ")");

        // 4. 将结果表执行写入
//        bobTable.executeInsert("output");
        userCountTable.executeInsert("user_count");
    }
}
