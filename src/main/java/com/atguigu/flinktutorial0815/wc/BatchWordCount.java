package com.atguigu.flinktutorial0815.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 创建一个执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取文件数据
        DataSource<String> dataSource = env.readTextFile("input/words.txt");

        // 定义转换流程，将每一行数据按空格分词，转换成(word, 1)二元组
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuples = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                // 按空格分割
                String[] words = line.split(" ");
                // 针对每个单词，包装成二元组输出
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        // 按照word做分组，位置索引为0
        UnsortedGrouping<Tuple2<String, Long>> groupBy = wordAndOneTuples.groupBy(0);

        // 统计同一单词的个数，做sum
        AggregateOperator<Tuple2<String, Long>> sum = groupBy.sum(1);

        // 打印输出
        sum.print();
    }
}
