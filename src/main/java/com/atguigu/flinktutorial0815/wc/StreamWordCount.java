package com.atguigu.flinktutorial0815.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = dataStreamSource.flatMap(
                   (String line, Collector<Tuple2<String, Long>> out) -> {
                        // 按空格分割
                        String[] words = line.split(" ");
                        // 针对每个单词，包装成二元组输出
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                )
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy( value -> value.f0 )
                .sum(1);

        sum.print();

        // 执行
        env.execute();
    }
}
