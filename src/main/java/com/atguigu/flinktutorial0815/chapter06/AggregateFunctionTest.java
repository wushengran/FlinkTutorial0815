package com.atguigu.flinktutorial0815.chapter06;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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

public class AggregateFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 统计10s内每个用户的访问频次
        stream.keyBy(value -> value.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate( new UserCountAgg() )
                .print();

        env.execute();
    }

    // 实现自定义的AggregateFunction，统计每个用户的访问频次
    public static class UserCountAgg implements AggregateFunction<ClickEvent, Tuple2<String, Long>, String>{
        @Override
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of("", 0L);
        }

        @Override
        public Tuple2<String, Long> add(ClickEvent value, Tuple2<String, Long> accumulator) {
            return Tuple2.of(value.user, accumulator.f1 + 1);
        }

        @Override
        public String getResult(Tuple2<String, Long> accumulator) {
            return "用户" + accumulator.f0 + "访问频次为：" + accumulator.f1;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
            return null;
        }
    }

    // 统计平均时间戳
    public static class AvgTs implements AggregateFunction<ClickEvent, Tuple2<Long, Integer>, String>{
        @Override
        public Tuple2<Long, Integer> createAccumulator() {
            return Tuple2.of(0L, 0);
        }

        @Override
        public Tuple2<Long, Integer> add(ClickEvent value, Tuple2<Long, Integer> accumulator) {
            return Tuple2.of(accumulator.f0 + value.ts, accumulator.f1 + 1);
        }

        @Override
        public String getResult(Tuple2<Long, Integer> accumulator) {
            return "平均时间戳为：" + (accumulator.f0 / accumulator.f1);
        }

        @Override
        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
            return null;
        }
    }
}
