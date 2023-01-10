package com.atguigu.flinktutorial0815.chapter08;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
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

public class SplitStreamTest_Filter {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<ClickEvent> stream = env.addSource(new ClickEventSource());

        // 用过滤的方法进行分流操作
        SingleOutputStreamOperator<ClickEvent> aliceStream = stream.filter(value -> value.user.equals("Alice"));
        SingleOutputStreamOperator<ClickEvent> bobStream = stream.filter(value -> value.user.equals("Bob"));
        SingleOutputStreamOperator<ClickEvent> elseStream = stream.filter(value -> !value.user.equals("Alice") && !value.user.equals("Bob"));

        aliceStream.print("alice");
        bobStream.print("bob");
        elseStream.print("else");

        env.execute();
    }
}
