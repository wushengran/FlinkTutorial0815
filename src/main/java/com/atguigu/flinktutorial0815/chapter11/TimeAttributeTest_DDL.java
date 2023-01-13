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

public class TimeAttributeTest_DDL {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        tableEnv.executeSql("create table clicks (" +
                " uname varchar(20)," +
                " url string," +
                " ts bigint, " +
//                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts/1000) )," +
                " et AS TO_TIMESTAMP_LTZ(ts, 3)," +    // 定义事件时间属性字段
                " WATERMARK FOR et AS et - INTERVAL '2' SECOND, " +    // 定义水位线生成策略
                " pt AS PROCTIME()" +    // 定义处理时间属性字段
                ") with (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")");
        tableEnv.from("clicks").printSchema();
    }
}
