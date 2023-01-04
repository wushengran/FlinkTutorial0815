package com.atguigu.flinktutorial0815.chapter05.Transformation;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class FilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取自定义数据源
        DataStreamSource<ClickEvent> input = env.addSource(new ClickEventSource());

        input
//                .filter(new FilterFunction<ClickEvent>() {
//                    @Override
//                    public boolean filter(ClickEvent value) throws Exception {
//                        return value.user.equals("Mary");
//                    }
//                })
                .filter( value -> value.user.equals("Bob") )
                .print();

        env.execute();
    }
}
