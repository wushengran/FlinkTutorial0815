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

public class SinkToKafkaTest {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        // 定义连接器表，从文件读取数据
        tableEnv.executeSql("create table clicks " +
                "(" +
                "  uname string, " +
                "  url string, " +
                "  ts bigint" +
                ") with (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'clicks'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
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
                "  'connector' = 'kafka'," +
                "  'topic' = 'bob_clicks'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'format' = 'csv'" +
                ")"
        );

        tableEnv.executeSql("create table user_count " +
                "(" +
                "  uname string, " +
                "  cnt bigint, " +
                "  PRIMARY KEY (uname) NOT ENFORCED" +
                ") with (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = 'user_count'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'key.format' = 'csv'," +
                "  'value.format' = 'csv'" +
                ")"
        );

        // SQL查询和写入
        tableEnv.executeSql("insert into bob_clicks select url, uname, ts from clicks where uname = 'Bob'");
        tableEnv.executeSql("insert into user_count select uname, count(*) as cnt from clicks group by uname");
    }
}
