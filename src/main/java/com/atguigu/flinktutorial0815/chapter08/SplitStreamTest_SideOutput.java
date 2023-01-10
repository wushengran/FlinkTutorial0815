package com.atguigu.flinktutorial0815.chapter08;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class SplitStreamTest_SideOutput {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<ClickEvent> stream = env.addSource(new ClickEventSource());

        // 定义输出标签
        OutputTag<ClickEvent> aliceTag = new OutputTag<ClickEvent>("alice") {};
        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("bob") {};

        // 用侧输出流的方法进行分流操作
        SingleOutputStreamOperator<ClickEvent> elseStream = stream.process(new ProcessFunction<ClickEvent, ClickEvent>() {
            @Override
            public void processElement(ClickEvent value, ProcessFunction<ClickEvent, ClickEvent>.Context ctx, Collector<ClickEvent> out) throws Exception {
                if (value.user.equals("Alice"))
                    ctx.output(aliceTag, value);
                else if (value.user.equals("Bob"))
                    ctx.output(bobTag, Tuple3.of(value.user, value.url, value.ts));
                else
                    out.collect(value);
            }
        });

        elseStream.getSideOutput(aliceTag).print("alice");
        elseStream.getSideOutput(bobTag).print("bob");
        elseStream.print("else");

        env.execute();
    }
}
