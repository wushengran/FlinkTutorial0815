package com.atguigu.flinktutorial0815.chapter05.Transformation;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class FlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取自定义数据源
        DataStreamSource<ClickEvent> input = env.addSource(new ClickEventSource());

        input
//                .flatMap(new FlatMapFunction<ClickEvent, String>() {
//                    @Override
//                    public void flatMap(ClickEvent value, Collector<String> out) throws Exception {
//                        if (value.user.equals("Alice")) {
//                            out.collect(value.user + " -> " + value.url);
//                        } else if (value.user.equals("Bob")) {
//                            out.collect(value.user);
//                            out.collect(value.url);
//                        }
//                    }
//                })
                .flatMap((ClickEvent value, Collector<String> out) -> {
                        if (value.user.equals("Alice")) {
                            out.collect(value.user + " -> " + value.url);
                        } else if (value.user.equals("Bob")) {
                            out.collect(value.user);
                            out.collect(value.url);
                        }
                    })
                .returns(Types.STRING)
                .print();

        env.execute();

    }
}
