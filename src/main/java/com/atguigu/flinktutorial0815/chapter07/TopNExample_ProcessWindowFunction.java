package com.atguigu.flinktutorial0815.chapter07;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class TopNExample_ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取测试数据源
        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 将所有数据分到同一组，用全窗口函数实现统计和排序，提取Top 2
        stream.keyBy(value -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new TopNProcessWindowResult(2))
                .print();

        env.execute();
    }

    // 实现自定义的处理窗口函数
    public static class TopNProcessWindowResult extends ProcessWindowFunction<ClickEvent, String, Boolean, TimeWindow>{
        private int n;

        public TopNProcessWindowResult(int n) {
            this.n = n;
        }

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<ClickEvent, String, Boolean, TimeWindow>.Context context, Iterable<ClickEvent> elements, Collector<String> out) throws Exception {
            // 用一个HashMap保存每个url的count值
            HashMap<String, Long> urlCountMap = new HashMap<>();

            // 遍历所有数据，统计url的count值
            for (ClickEvent element : elements) {
                Long count = urlCountMap.getOrDefault(element.url, 0L);
                urlCountMap.put(element.url, count + 1);
            }

            // 为了方便排序，用一个list保存所有(url, count)
            ArrayList<Tuple2<String, Long>> urlCountList = new ArrayList<>();
            for (String url : urlCountMap.keySet()) {
                urlCountList.add(Tuple2.of(url, urlCountMap.get(url)));
            }

            // 排序
            urlCountList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return Long.compare(o2.f1, o1.f1);
                }
            });

            // 提取信息，包装成String，打印输出
            long start = context.window().getStart();
            long end = context.window().getEnd();
            StringBuilder builder = new StringBuilder();
            builder.append("-------------------------------\n")
                    .append("窗口 [" + start + " ~ " + end + ") 的 Top " + n + " 统计结果为：\n");

            // 遍历urlCountList，提取 top n
            for (int i = 0; i < Math.min(n, urlCountList.size()); i++){
                Tuple2<String, Long> urlCount = urlCountList.get(i);
                builder.append("NO." + (i+1) +
                        " URL:" + urlCount.f0 +
                        " 热门度：" + urlCount.f1 + "\n"
                );
            }
            builder.append("-------------------------------\n");

            out.collect(builder.toString());
        }
    }
}
