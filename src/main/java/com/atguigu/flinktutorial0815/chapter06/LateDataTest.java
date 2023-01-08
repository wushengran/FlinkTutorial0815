package com.atguigu.flinktutorial0815.chapter06;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class LateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
        SingleOutputStreamOperator<ClickEvent> stream = env.socketTextStream("hadoop102", 7777)
                .map( value -> {
                    String[] fields = value.split(",");
                    return new ClickEvent(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                } )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 定义一个输出标签，用来表示迟到数据的侧输出流
        OutputTag<ClickEvent> outputTag = new OutputTag<ClickEvent>("late") {
        };

        // 统计10s内每个url的访问频次
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(value -> value.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag)
                .aggregate(new UrlViewCountExample.UrlViewCountAgg(), new UrlViewCountExample.UrlCountWindowResult());
        urlCountStream.print();
        urlCountStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
