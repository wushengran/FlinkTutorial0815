package com.atguigu.flinktutorial0815.chapter08;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取不同类型的两条流
        SingleOutputStreamOperator<ClickEvent> stream1 = env.fromElements(
                new ClickEvent("Alice", "./home", 1000L),
                new ClickEvent("Bob", "./cart", 2000L),
                new ClickEvent("Alice", "./home", 3000L),
                new ClickEvent("Bob", "./fav", 5000L),
                new ClickEvent("Cary", "./home", 8000L),
                new ClickEvent("Bob", "./cart", 10000L),
                new ClickEvent("Cary", "./home", 12000L),
                new ClickEvent("Alice", "./home", 15000L),
                new ClickEvent("Mary", "./home", 18000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.ts)
        );

        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> stream2 = env.fromElements(
                Tuple3.of("Alice", 20, 2000L),
                Tuple3.of("Bob", 25, 5000L),
                Tuple3.of("Bob", 29, 6000L),
                Tuple3.of("Alice", 10, 11000L),
                Tuple3.of("Cary", 15, 16000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.f2)
        );

        // 间隔连接
        stream1.keyBy(value -> value.user)
                .intervalJoin(stream2.keyBy(value -> value.f0))
                .between(Time.seconds(-5), Time.seconds(3))
                .process(new ProcessJoinFunction<ClickEvent, Tuple3<String, Integer, Long>, String>() {
                    @Override
                    public void processElement(ClickEvent left, Tuple3<String, Integer, Long> right, ProcessJoinFunction<ClickEvent, Tuple3<String, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + " -> " + right);
                    }
                })
                .print();

        env.execute();
    }
}
