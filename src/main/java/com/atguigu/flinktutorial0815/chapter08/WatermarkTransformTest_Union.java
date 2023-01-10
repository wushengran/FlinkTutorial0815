package com.atguigu.flinktutorial0815.chapter08;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class WatermarkTransformTest_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取两条socket文本流
        SingleOutputStreamOperator<ClickEvent> stream1 = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] fields = value.split(",");
                    return new ClickEvent(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        SingleOutputStreamOperator<ClickEvent> stream2 = env.socketTextStream("hadoop102", 7778)
                .map(value -> {
                    String[] fields = value.split(",");
                    return new ClickEvent(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        // 两条流做联合
        stream1.union(stream2)
                .process(new ProcessFunction<ClickEvent, String>() {
                    @Override
                    public void processElement(ClickEvent value, ProcessFunction<ClickEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("当前数据为：" + value);
                        out.collect("当前水位线为：" + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}
