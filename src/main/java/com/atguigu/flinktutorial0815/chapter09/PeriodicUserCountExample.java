package com.atguigu.flinktutorial0815.chapter09;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class PeriodicUserCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 利用自定义状态和定时器，统计每个用户的访问频次，并周期性地输出
        stream.keyBy(value -> value.user)
                .process(new PeriodicUserCountResult())
                .print();

        env.execute();
    }

    // 实现自定义的KeyedProcessFunction
    public static class PeriodicUserCountResult extends KeyedProcessFunction<String, ClickEvent, String>{
        // 自定义值状态，保存当前用户的访问频次
        private ValueState<Long> countState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Types.LONG));
        }

        @Override
        public void processElement(ClickEvent value, KeyedProcessFunction<String, ClickEvent, String>.Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，count加1
            if (countState.value() == null)
                countState.update(1L);
            else
                countState.update(countState.value() + 1);

            // 如果没有注册过定时器，就注册定时器
            if (timerTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000L);
                timerTsState.update(ctx.timestamp() + 10000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, ClickEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出当前统计结果
            out.collect("用户" + ctx.getCurrentKey() + "的当前访问频次为：" + countState.value());

            // 直接注册下一个定时器
            ctx.timerService().registerEventTimeTimer(timestamp + 10000L);
            timerTsState.update(timestamp + 10000L);

            // 清空定时器状态
//            timerTsState.clear();
        }
    }
}
