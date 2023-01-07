package com.atguigu.flinktutorial0815.chapter06;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 开10s滚动窗口，统计每个用户最近一次访问
//        stream.keyBy(value -> value.user)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .maxBy("ts")
//                .print("max ts")
//        ;

//        stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .maxBy("ts")
//                .print("max ts");

        // 不同的窗口分配器
        // 统计每个用户访问频次
        stream.map(value -> Tuple2.of(value.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))   // 滚动事件时间窗口
//                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8))))    // 滚动处理时间窗口
//                .window(SlidingEventTimeWindows.of(Time.days(1), Time.hours(1)))     // 滑动事件时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(5)))       // 事件时间会话窗口
//                .window(ProcessingTimeSessionWindows.withDynamicGap(element -> element.ts % 100000)     // 处理时间会话窗口
//                .countWindow(10)    // 滚动计数窗口，每10条数据统计一次
//                .countWindow(10, 2)    // 滑动计数窗口，统计10条数据，每隔2个数统计一次
                .sum("f1")
                .print("user count");

        env.execute();
    }
}
