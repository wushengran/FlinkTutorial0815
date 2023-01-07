package com.atguigu.flinktutorial0815.chapter06;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class ProcessWindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 定义一个输出标签，用来表示侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("watermark") {
        };

        // 统计10s内每个用户的访问频次，使用全窗口函数实现
        SingleOutputStreamOperator<String> result = stream.keyBy(value -> value.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<ClickEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(String user, ProcessWindowFunction<ClickEvent, String, String, TimeWindow>.Context context, Iterable<ClickEvent> elements, Collector<String> out) throws Exception {
                        Long count = 0L;
                        for (ClickEvent element : elements) count++;

                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        out.collect("窗口 [" + start + " ~ " + end + ") "
                                + "用户" + user + "的访问频次为：" + count);

                        // 将水位线信息输出到侧输出流
                        context.output(outputTag, "水位线为：" + context.currentWatermark());
                    }
                });
        result.print();
        result.getSideOutput(outputTag).print("wm");

        env.execute();
    }
}
