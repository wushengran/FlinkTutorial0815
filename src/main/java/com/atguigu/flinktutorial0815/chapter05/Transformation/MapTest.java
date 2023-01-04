package com.atguigu.flinktutorial0815.chapter05.Transformation;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class MapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取自定义数据源
        DataStreamSource<ClickEvent> input = env.addSource(new ClickEventSource());

        // 1. 传入实现了MapFunction接口的自定义类的对象
        SingleOutputStreamOperator<String> stream1 = input.map(new MyMapper());

        // 2. 传入匿名类的对象
        SingleOutputStreamOperator<Tuple2<String, String>> stream2 = input.map(new MapFunction<ClickEvent, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(ClickEvent value) throws Exception {
                return Tuple2.of(value.user, value.url);
            }
        });

        // 3. 传入Lambda表达式
        SingleOutputStreamOperator<String> stream3 = input.map(value -> value.url);

        // 如果返回是泛型类型，需要指明返回类型
        SingleOutputStreamOperator<Tuple2<String, String>> stream4 = input.map(value -> Tuple2.of(value.user, value.url))
                .returns(Types.TUPLE(Types.STRING, Types.STRING));

        SingleOutputStreamOperator<Tuple2<String, String>> stream5 = input.map(value -> Tuple2.of(value.user, value.url), Types.TUPLE(Types.STRING, Types.STRING));

        stream5.print();

        env.execute();
    }

    public static class MyMapper implements  MapFunction<ClickEvent, String>{
        @Override
        public String map(ClickEvent value) throws Exception {
            return value.user;
        }
    }
}
