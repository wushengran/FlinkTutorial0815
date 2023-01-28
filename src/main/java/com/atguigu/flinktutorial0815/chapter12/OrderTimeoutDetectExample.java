package com.atguigu.flinktutorial0815.chapter12;

import com.atguigu.flinktutorial0815.chapter08.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class OrderTimeoutDetectExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取订单事件流
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.fromElements(
                new OrderEvent("order_1", "user_1", "create", 1000L),
                new OrderEvent("order_2", "user_2", "create", 2000L),
                new OrderEvent("order_1", "user_1", "modify", 10 * 1000L),
                new OrderEvent("order_1", "user_1", "pay", 60 * 1000L),
                new OrderEvent("order_3", "user_2", "create", 10 * 60 * 1000L),
                new OrderEvent("order_3", "user_2", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.ts)
        );

        // 2. 定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.status.equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.status.equals("pay");
                    }
                })
                .within(Time.minutes(15));

        // 3. 将模式应用在流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(value -> value.orderId), pattern);

        // 4. 定义输出标签
        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };

        // 5. 检测处理匹配事件和超时部分匹配事件
        SingleOutputStreamOperator<String> resultStream = patternStream.process(new OrderTimeDetect());

        resultStream.print("payed");
        resultStream.getSideOutput(outputTag).print("timeout");

        env.execute();
    }

    public static class OrderTimeDetect extends PatternProcessFunction<OrderEvent, String>
       implements TimedOutPartialMatchHandler<OrderEvent>{
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            OrderEvent create = match.get("create").get(0);
            OrderEvent pay = match.get("pay").get(0);
            out.collect("订单 " + create.orderId + " 支付成功!");
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            OrderEvent create = match.get("create").get(0);
            ctx.output( new OutputTag<String>("timeout") {},
                    "订单 " + create.orderId + " 超时未支付!");
        }
    }
}
