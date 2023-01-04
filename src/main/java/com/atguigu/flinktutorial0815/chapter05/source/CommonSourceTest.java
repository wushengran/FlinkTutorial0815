package com.atguigu.flinktutorial0815.chapter05.source;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class CommonSourceTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从元素读取数据
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<String> stream2 = env.fromElements("a", "b", "c");

        // 2. 从集合读取数据
        ArrayList<Long> list = new ArrayList<>();
        list.add(1L);
        list.add(24L);
        DataStreamSource<Long> stream3 = env.fromCollection(list);

        ArrayList<ClickEvent> events = new ArrayList<>();
        events.add(new ClickEvent("Alice", "./home", 1000L));
        events.add(new ClickEvent("Bob", "./prod?id=1", 2000L));
        events.add(new ClickEvent("Cary", "./prod?id=2", 5000L));
        DataStreamSource<ClickEvent> stream4 = env.fromCollection(events);

        // 3. 从文件读取
        DataStreamSource<String> stream5 = env.readTextFile("input/clicks.txt");

        // 4. 从socket文本流读取
        DataStreamSource<String> stream6 = env.socketTextStream("hadoop102", 7777);

        stream5.print();

        env.execute();
    }
}
