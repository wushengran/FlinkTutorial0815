package com.atguigu.flinktutorial0815.chapter05.Transformation;

import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class PartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickEventSource())
//                .shuffle()
//                .rebalance()
//                .broadcast()
                .global()
//                .partitionCustom()
                .print().setParallelism(4);

        env.execute();
    }
}
