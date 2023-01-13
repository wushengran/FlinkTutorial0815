package com.atguigu.flinktutorial0815.chapter11;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

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
        // 直接基于注册在环境中的表，得到一个Table对象
        Table clickTable = tableEnv.from("clicks");
        clickTable.printSchema();

        // 2. 查询转换
        Table bobTable = tableEnv.sqlQuery("select url, uname, ts from clicks where uname = 'Bob'");
        Table userCountTable = tableEnv.sqlQuery("select uname, count(*) as cnt from clicks group by uname");

        // 基于 Table 对象继续查询转换
        // 2.1 调用table api
        Table bobHomeTable = bobTable.where($("url").isEqual(" ./home"))
                .select($("uname"), $("url"));

        // 2.2 SQL
        // 需要先在表环境中注册表
        tableEnv.createTemporaryView("bob_clicks", bobTable);
        Table bobHomeTable2 = tableEnv.sqlQuery("select url, uname, ts from bob_clicks where url = ' ./home'");

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
        bobHomeTable2.executeInsert("output");
//        userCountTable.executeInsert("user_count");
    }
}
