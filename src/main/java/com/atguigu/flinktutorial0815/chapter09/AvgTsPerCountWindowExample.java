package com.atguigu.flinktutorial0815.chapter09;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class AvgTsPerCountWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 模拟计数窗口，统计每个用户最近5次访问的平均时间戳
        stream.keyBy(value -> value.user)
                .flatMap(new AvgTsPerCountWindow(5))
                .print();

        env.execute();
    }

    public static class AvgTsPerCountWindow extends RichFlatMapFunction<ClickEvent, String> {
        private int size;

        public AvgTsPerCountWindow(int size) {
            this.size = size;
        }

        // 自定义聚合状态，输出(count, avg_ts)
        private AggregatingState<ClickEvent, Tuple2<Integer, Long>> avgTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义状态，中间累加状态为(sum, count)
            avgTsState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<ClickEvent, Tuple2<Long, Integer>, Tuple2<Integer, Long>>(
                    "avg-ts",
                    new AggregateFunction<ClickEvent, Tuple2<Long, Integer>, Tuple2<Integer, Long>>() {
                        @Override
                        public Tuple2<Long, Integer> createAccumulator() {
                            // 初始化状态，保存（sum，count）
                            return Tuple2.of(0L, 0);
                        }

                        @Override
                        public Tuple2<Long, Integer> add(ClickEvent value, Tuple2<Long, Integer> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.ts, accumulator.f1 + 1);
                        }

                        @Override
                        public Tuple2<Integer, Long> getResult(Tuple2<Long, Integer> accumulator) {
                            return Tuple2.of( accumulator.f1, accumulator.f0 / accumulator.f1 );
                        }

                        @Override
                        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.INT)
            ));
        }

        @Override
        public void flatMap(ClickEvent value, Collector<String> out) throws Exception {
            // 每来一条数据，直接在状态中聚合
            avgTsState.add(value);

            // 判断是否达到计数窗口size，达到就输出
            if (avgTsState.get().f0 >= size){
                out.collect("用户" + value.user + "最近" + size + "次访问平均时间戳为：" + avgTsState.get().f1);
                avgTsState.clear();
            }
        }
    }
}
