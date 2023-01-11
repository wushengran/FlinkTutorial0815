package com.atguigu.flinktutorial0815.chapter09;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import com.atguigu.flinktutorial0815.chapter06.UrlViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

public class SimulatedTumblingWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 模拟滚动窗口，统计10s内每个url的访问频次
        stream.keyBy(value -> value.url)
                .process(new SimulatedTumblingWindow(10000L))
                .print();

        env.execute();
    }

    // 实现自定义的KeyedProcessFunction
    public static class SimulatedTumblingWindow extends KeyedProcessFunction<String, ClickEvent, UrlViewCount>{
        private Long size;

        public SimulatedTumblingWindow(Long size) {
            this.size = size;
        }

        // 自定义MapState，保存(window_start, count)
        private MapState<Long, Long> windowCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window_count", Long.class, Long.class));
        }

        @Override
        public void processElement(ClickEvent value, KeyedProcessFunction<String, ClickEvent, UrlViewCount>.Context ctx, Collector<UrlViewCount> out) throws Exception {
            // 根据数据时间戳确定属于哪个窗口，实现窗口分配器的功能
            Long start = ctx.timestamp() - ctx.timestamp() % size;
            Long end = start + size;

            // 更新MapState中的值，实现增量聚合的功能
            if (windowCountMapState.contains(start))
                windowCountMapState.put(start, windowCountMapState.get(start) + 1);
            else
                windowCountMapState.put(start, 1L);

            // 注册定时器，等待窗口触发
            ctx.timerService().registerEventTimeTimer(end - 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, ClickEvent, UrlViewCount>.OnTimerContext ctx, Collector<UrlViewCount> out) throws Exception {
            // 定时器触发，执行窗口计算，输出结果
            String url = ctx.getCurrentKey();
            Long end = timestamp + 1;
            Long start = end - size;
            Long count = windowCountMapState.get(start);

            out.collect(new UrlViewCount(url, count, start, end));

            // 关闭窗口
            windowCountMapState.remove(start);
        }
    }
}

