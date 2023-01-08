package com.atguigu.flinktutorial0815.chapter07;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        // 定义一个输出标签
        OutputTag<String> outputTag = new OutputTag<String>("time") {
        };

        SingleOutputStreamOperator<String> process = stream
                .process(new ProcessFunction<ClickEvent, String>() {
            @Override
            public void processElement(ClickEvent value, ProcessFunction<ClickEvent, String>.Context ctx, Collector<String> out) throws Exception {
                // 全覆盖flatmap功能
                out.collect(value.user);
                out.collect(value.url + ", " + value.ts);

                // 覆盖聚合功能，需要先keyBy，然后使用状态

                // 覆盖窗口功能，需要定时器操作，必须先keyBy
//                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000L);

                // 获取当前时间信息，侧输出流输出
                ctx.output(outputTag, "当前数据时间戳为：" + ctx.timestamp());
                ctx.output(outputTag, "当前处理时间为：" + ctx.timerService().currentProcessingTime());
                ctx.output(outputTag, "当前水位线为：" + ctx.timerService().currentWatermark());
            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<ClickEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                // 定时器触发，要做的操作
                out.collect("定时器触发！当前时间戳为：" + timestamp);
            }
        });
        process.print();
        process.getSideOutput(outputTag).print("time");

        env.execute();
    }
}
