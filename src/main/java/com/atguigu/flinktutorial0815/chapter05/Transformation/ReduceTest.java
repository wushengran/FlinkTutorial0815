package com.atguigu.flinktutorial0815.chapter05.Transformation;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取自定义数据源
        DataStreamSource<ClickEvent> input = env.addSource(new ClickEventSource());

        // 统计每个用户最近一次访问时间戳，以及之前访问过的所有页面
        input.keyBy(event -> event.user)
                .reduce((data1, data2) -> {
                    return new ClickEvent(data1.user, data1.url + ", " + data2.url, data2.ts);
                })
                .print();

        env.execute();
    }
}
