package com.atguigu.flinktutorial0815.chapter05.Transformation;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
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

public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<Integer> input = env.fromElements(1, 2, 3, 4, 5, 6);

        input.flatMap(new RichFlatMapFunction<Integer, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("任务开始！");
                    }

                    @Override
                    public void flatMap(Integer value, Collector<String> out) throws Exception {
                        out.collect("当前数据为：" + value +
                                "当前任务为：" + getRuntimeContext().getTaskName() + " " + getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("任务结束！");
                    }
                })
                .print();

        env.execute();
    }
}
