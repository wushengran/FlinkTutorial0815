package com.atguigu.flinktutorial0815.chapter08;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取不同类型的两条流
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stream2 = env.fromElements("a", "b", "c", "d");

        // 合流
        stream1.connect(stream2)
                .map(new CoMapFunction<Integer, String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map1(Integer value) throws Exception {
                        return Tuple2.of("number", value);
                    }

                    @Override
                    public Tuple2<String, Integer> map2(String value) throws Exception {
                        return Tuple2.of(value, 0);
                    }
                })
                .print();

        env.execute();
    }
}
