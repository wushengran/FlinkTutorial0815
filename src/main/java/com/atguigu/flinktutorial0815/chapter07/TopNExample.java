package com.atguigu.flinktutorial0815.chapter07;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import com.atguigu.flinktutorial0815.chapter06.UrlViewCount;
import com.atguigu.flinktutorial0815.chapter06.UrlViewCountExample;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(value -> value.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlViewCountExample.UrlViewCountAgg(), new UrlViewCountExample.UrlCountWindowResult());

        urlCountStream.print("url count");

        // 按照窗口分组，直接注册定时器统计Top N
        urlCountStream.keyBy(value -> value.end)
                .process(new TopNUrl(3))
                .print();

        env.execute();
    }

    // 实现自定义的KeyedProcessFunction
    public static class TopNUrl extends KeyedProcessFunction<Long, UrlViewCount, String>{
        private int n;

        public TopNUrl(int n) {
            this.n = n;
        }

        // 定义一个列表状态，用来保存所有的数据
        private ListState<Tuple2<String, Long>> urlCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            urlCountListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("url-count-liststate", Types.TUPLE(Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，就保存到列表状态里
            urlCountListState.add(Tuple2.of(value.url, value.count));
            // 注册定时器，收集所有数据
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，收集到了所有数据，排序提取top n输出
            // 创建一个列表，用于排序
            ArrayList<Tuple2<String, Long>> urlCountList = new ArrayList<>();
            for (Tuple2<String, Long> urlCount : urlCountListState.get()) {
                urlCountList.add(urlCount);
            }

            // 排序
            urlCountList.sort( (o1, o2) -> Long.compare(o2.f1, o1.f1) );

            // 提取信息，包装成String，打印输出
            long end = ctx.getCurrentKey();
            long start = end - 10000L;
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
