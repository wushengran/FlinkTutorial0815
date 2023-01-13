package com.atguigu.flinktutorial0815.chapter11;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class TopNExample_SQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

        stream.print("input");

        // 将流转换成表，并指定事件时间属性字段
//        Table clickTable = tableEnv.fromDataStream(stream, $("user"), $("url"), $("ts"),
//                $("et").rowtime());
//        tableEnv.createTemporaryView("clicks", clickTable);
        tableEnv.createTemporaryView("clicks", stream, $("user"), $("url"), $("ts"),
                $("et").rowtime());

        // 1. 开滚动窗口，统计10s内每个url的访问量
        Table urlCountTable = tableEnv.sqlQuery("select url, count(*) as cnt, window_start, window_end " +
                " from TABLE(" +
                "    TUMBLE(TABLE clicks, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ")" +
                " group by url, window_start, window_end" +
                "");

        tableEnv.createTemporaryView("url_count", urlCountTable);

        // 2. 在每个窗口内，根据count值排序，开窗聚合出排名，筛选top n
        Table topNTable = tableEnv.sqlQuery("select * from " +
                "(" +
                "select url, cnt, window_start, window_end, " +
                " ROW_NUMBER() OVER(" +
                "   PARTITION BY window_start, window_end " +
                "   ORDER BY cnt DESC " +
                ") as row_num " +
                " from url_count " +
                ")" +
                " where row_num <= 3");

        // 直接全部用SQL实现
//        Table topNTable = tableEnv.sqlQuery("select * from " +
//                "(" +
//                " select url, cnt, window_start, window_end, " +
//                "  ROW_NUMBER() OVER(" +
//                "   PARTITION BY window_start, window_end " +
//                "   ORDER BY cnt DESC " +
//                "  ) as row_num " +
//                " from (" +
//                "   select url, count(*) as cnt, window_start, window_end " +
//                "     from TABLE(" +
//                "        TUMBLE(TABLE clicks, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
//                "     )" +
//                "   group by url, window_start, window_end" +
//                "   )" +
//                ")" +
//                " where row_num <= 3");

        tableEnv.toChangelogStream(topNTable).print();

        env.execute();
    }
}
