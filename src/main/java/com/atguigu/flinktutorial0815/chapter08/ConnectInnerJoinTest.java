package com.atguigu.flinktutorial0815.chapter08;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class ConnectInnerJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取不同类型的两条流
        SingleOutputStreamOperator<ClickEvent> stream1 = env.readTextFile("input/clicks.txt")
                .map(value -> {
                    String[] fields = value.split(",");
                    return new ClickEvent(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                });

        DataStreamSource<Tuple2<String, Integer>> stream2 = env.fromElements(
                Tuple2.of("Alice", 20),
                Tuple2.of("Bob", 25),
                Tuple2.of("Alice", 10),
                Tuple2.of("Cary", 15)
        );

        // 按照用户作为key进行连接操作
        stream1.connect(stream2)
                .keyBy(value -> value.user, value -> value.f0)
                .flatMap(new RichCoFlatMapFunction<ClickEvent, Tuple2<String, Integer>, String>() {
                    // 定义列表状态，分别保存两条流中已经到来的数据
                    private ListState<ClickEvent> eventListState;
                    private ListState<Tuple2<String, Integer>> tuple2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        eventListState = getRuntimeContext().getListState(new ListStateDescriptor<ClickEvent>("events", ClickEvent.class));
                        tuple2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Integer>>("tuples", Types.TUPLE(Types.STRING, Types.INT)));
                    }

                    @Override
                    public void flatMap1(ClickEvent event, Collector<String> out) throws Exception {
                        // 遍历第二条流的所有数据
                        for (Tuple2<String, Integer> tuple2 : tuple2ListState.get()) {
                            out.collect(event + " -> " + tuple2);
                        }
                        // 保存状态
                        eventListState.add(event);
                    }

                    @Override
                    public void flatMap2(Tuple2<String, Integer> tuple2, Collector<String> out) throws Exception {
                        // 遍历第一条流的所有数据
                        for (ClickEvent event : eventListState.get()) {
                            out.collect(event + " -> " + tuple2);
                        }
                        // 保存状态
                        tuple2ListState.add(tuple2);
                    }
                })
                .print();


        env.execute();
    }
}
