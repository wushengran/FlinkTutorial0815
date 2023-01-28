package com.atguigu.flinktutorial0815.chapter11;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class UDFTest_ScalarFunction {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

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

        // 注册一个自定义函数 myHash，计算字符串的哈希值
        tableEnv.createTemporarySystemFunction("myHash", MyHash.class);

        // 输出表
        tableEnv.executeSql("create table output " +
                "(" +
                "  uname string, " +
                "  url string, " +
                "  ts bigint, " +
                "  user_hash int, " +
                "  url_hash int" +
                ") with (" +
                "  'connector' = 'print'" +
                ")"
        );

        Table result = tableEnv.sqlQuery("select uname, url, ts, myHash(uname), myHash(url) from clicks");

        result.executeInsert("output");
    }

    // 实现自定义的标量函数
    public static class MyHash extends ScalarFunction{
        public int eval(String str){
            return str.hashCode();
        }
    }
}
