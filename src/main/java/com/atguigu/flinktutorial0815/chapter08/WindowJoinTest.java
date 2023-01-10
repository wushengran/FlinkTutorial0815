package com.atguigu.flinktutorial0815.chapter08;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class WindowJoinTest {
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

        // 统计10s内每个用户两条流的连接数据
        stream1.join(stream2)
                .where(value -> value.user)
                .equalTo(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<ClickEvent, Tuple3<String, Integer, Long>, String>() {
                    @Override
                    public String join(ClickEvent first, Tuple3<String, Integer, Long> second) throws Exception {
                        return first + " -> " + second;
                    }
                })
                .print();

        env.execute();
    }
}
