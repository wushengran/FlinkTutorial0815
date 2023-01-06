package com.atguigu.flinktutorial0815.chapter05.sink;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");

        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        // 将数据转换成ClickEvent
        SingleOutputStreamOperator<ClickEvent> events = stream.map(value -> {
            String[] fields = value.split(",");
            return new ClickEvent(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
        });

        // 写入kafka
        events.map(ClickEvent::toString).addSink( new FlinkKafkaProducer<String>("events", new SimpleStringSchema(), properties));

        env.execute();
    }
}
