package com.atguigu.flinktutorial0815.chapter07;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import com.atguigu.flinktutorial0815.chapter06.UrlViewCount;
import com.atguigu.flinktutorial0815.chapter06.UrlViewCountExample;
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

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class TopNExample_Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 统计10s内每个url的访问频次
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(value -> value.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlViewCountExample.UrlViewCountAgg(), new UrlViewCountExample.UrlCountWindowResult());

        urlCountStream.print("url count");

        // 继续开窗，收集每个10s内输出的所有UrlViewCount
        urlCountStream.keyBy(value -> value.start)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process( new WindowTopNResult(3) )
                .print();

        env.execute();
    }

    // 实现自定义的处理窗口函数
    public static class WindowTopNResult extends ProcessWindowFunction<UrlViewCount, String, Long, TimeWindow>{
        private int n;

        public WindowTopNResult(int n) {
            this.n = n;
        }

        @Override
        public void process(Long aLong, ProcessWindowFunction<UrlViewCount, String, Long, TimeWindow>.Context context, Iterable<UrlViewCount> elements, Collector<String> out) throws Exception {
            // 为了方便排序，用一个list保存所有(url, count)
            ArrayList<Tuple2<String, Long>> urlCountList = new ArrayList<>();
            for (UrlViewCount element : elements) {
                urlCountList.add(Tuple2.of(element.url, element.count));
            }

            // 排序
            urlCountList.sort((o1, o2) -> Long.compare(o2.f1, o1.f1));

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
