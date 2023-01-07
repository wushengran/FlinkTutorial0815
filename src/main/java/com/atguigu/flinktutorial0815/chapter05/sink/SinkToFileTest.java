package com.atguigu.flinktutorial0815.chapter05.sink;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class SinkToFileTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 从文件读取数据
        DataStreamSource<String> input = env.readTextFile("input/clicks.txt");

        // 将String类型的日志数据，转换成ClickEvent
        SingleOutputStreamOperator<ClickEvent> events = input.map(value -> {
            String[] fields = value.split(",");
            return new ClickEvent(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
        });

        // 写入文件
//        events.writeAsText("output/events.txt");

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("hdfs://hadoop102:8020/test"), new SimpleStringEncoder<>())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build()
                )
                .build();

        events.map(ClickEvent::toString).addSink(streamingFileSink);

        env.execute();
    }
}
