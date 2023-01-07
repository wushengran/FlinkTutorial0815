package com.atguigu.flinktutorial0815.chapter05.Transformation;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class MostActiveUserExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<ClickEvent> input = env.addSource(new ClickEventSource());

        input.print("input");

        // 统计当前最活跃用户
        // 1. 统计每个用户的访问频次，作为活跃度
        SingleOutputStreamOperator<Tuple2<String, Long>> userCountStream = input.map(value -> Tuple2.of(value.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f0)
//                .sum("f1");
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1));

        userCountStream.print("user count");

        // 2. 选取活跃度最大的数据
        SingleOutputStreamOperator<Tuple2<String, Long>> mostActiveUser = userCountStream.keyBy(value -> true)
//                .maxBy("f1");
                .reduce((value1, value2) -> {
                    return value1.f1 > value2.f1 ? value1 : value2;
                });
        mostActiveUser.print("most active user");

        env.execute();
    }
}
