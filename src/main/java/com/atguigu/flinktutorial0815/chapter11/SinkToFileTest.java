package com.atguigu.flinktutorial0815.chapter11;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class SinkToFileTest {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        // 定义连接器表，从文件读取数据
        tableEnv.executeSql("create table clicks " +
                "(" +
                "  uname string, " +
                "  url string, " +
                "  ts bigint" +
                ") with (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'input/clicks.txt'," +
                "  'format' = 'csv'" +
                ")"
        );

        // 定义连接器表，收集输出的数据
        tableEnv.executeSql("create table bob_clicks " +
                "(" +
                "  url string, " +
                "  uname string, " +
                "  ts bigint" +
                ") with (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'output'," +
                "  'format' = 'csv'" +
                ")"
        );

        // SQL查询和写入
        tableEnv.executeSql("insert into bob_clicks select url, uname, ts from clicks where uname = 'Bob'");
    }
}
