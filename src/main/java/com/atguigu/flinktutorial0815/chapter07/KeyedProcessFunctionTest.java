package com.atguigu.flinktutorial0815.chapter07;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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

public class KeyedProcessFunctionTest {
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

        SingleOutputStreamOperator<String> process = stream.keyBy(value -> value.user)
                .process(new KeyedProcessFunction<String, ClickEvent, String>() {
                    @Override
                    public void processElement(ClickEvent value, KeyedProcessFunction<String, ClickEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("------------------\n");
                        out.collect("当前数据为：" + value);

                        // 获取当前时间信息，侧输出流输出
                        ctx.output(outputTag, "当前数据时间戳为：" + ctx.timestamp());
                        ctx.output(outputTag, "当前处理时间为：" + ctx.timerService().currentProcessingTime());
                        ctx.output(outputTag, "当前水位线为：" + ctx.timerService().currentWatermark());

                        // 注册定时器
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10000L);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, ClickEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("*******************\n");
                        out.collect("定时器触发！当前时间戳为：" + timestamp);
                        out.collect("当前时间语义为：" + ctx.timeDomain());
                        out.collect("当前用户为：" + ctx.getCurrentKey());

                        ctx.output(outputTag, "当前处理时间为：" + ctx.timerService().currentProcessingTime());
                        ctx.output(outputTag, "当前水位线为：" + ctx.timerService().currentWatermark());
                    }
                });
        process.print();
        process.getSideOutput(outputTag).print("time");

        env.execute();
    }
}
