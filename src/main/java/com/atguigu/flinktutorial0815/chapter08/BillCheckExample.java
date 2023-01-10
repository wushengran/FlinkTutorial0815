package com.atguigu.flinktutorial0815.chapter08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取订单事件流
        SingleOutputStreamOperator<OrderEvent> orderStream = env.fromElements(
                        new OrderEvent("order-0001", "Alice", "create", 1000L),
                        new OrderEvent("order-0002", "Alice", "create", 2000L),
                        new OrderEvent("order-0001", "Alice", "modify", 5000L),
                        new OrderEvent("order-0002", "Alice", "modify", 8000L),
                        new OrderEvent("order-0001", "Alice", "pay", 10000L),
                        new OrderEvent("order-0003", "Bob", "create", 15000L),
                        new OrderEvent("order-0003", "Bob", "pay", 18000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                )
                .filter(value -> value.status.equals("pay"));

        // 读取第三方支付平台到账事件流
        SingleOutputStreamOperator<ThirdpartyPayEvent> thirdpartyPayStream = env.fromElements(
                new ThirdpartyPayEvent("order-0001", 256.0, "alipay", 12000L),
                new ThirdpartyPayEvent("order-0002", 89.0, "wechat", 19000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<ThirdpartyPayEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.ts)
        );

        // 定义一个输出标签，用来保存报警信息
        OutputTag<String> outputTag = new OutputTag<String>("warning") {};

        // 连接两条流
        SingleOutputStreamOperator<String> result = orderStream.connect(thirdpartyPayStream)
                .keyBy(value -> value.orderId, value -> value.orderId)
                .process(new BillCheckResult());

        result.print();
        result.getSideOutput(outputTag).print("warning");

        env.execute();
    }

    // 实现自定义的处理函数
    public static class BillCheckResult extends KeyedCoProcessFunction<String, OrderEvent, ThirdpartyPayEvent, String>{
        // 定义值状态，保存已经到来的两条流中的事件
        private ValueState<OrderEvent> orderPayState;
        private ValueState<ThirdpartyPayEvent> thirdpartyPayState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderPayState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-pay", OrderEvent.class));
            thirdpartyPayState = getRuntimeContext().getState(new ValueStateDescriptor<ThirdpartyPayEvent>("thirdparty-pay", ThirdpartyPayEvent.class));
        }

        @Override
        public void processElement1(OrderEvent orderPay, KeyedCoProcessFunction<String, OrderEvent, ThirdpartyPayEvent, String>.Context ctx, Collector<String> out) throws Exception {
            // 判断另一条流中对应事件是否到来
            ThirdpartyPayEvent thirdpartyPay = thirdpartyPayState.value();
            if ( thirdpartyPay != null){
                // 如果已经到来，匹配成功，直接输出信息，清空状态
                out.collect("订单" + ctx.getCurrentKey() + "对账成功！" + orderPay + " -> " + thirdpartyPay);
                thirdpartyPayState.clear();
            } else {
                // 如果没来，注册定时器开始等待，保存状态
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000L);
                orderPayState.update(orderPay);
            }
        }

        @Override
        public void processElement2(ThirdpartyPayEvent thirdpartyPay, KeyedCoProcessFunction<String, OrderEvent, ThirdpartyPayEvent, String>.Context ctx, Collector<String> out) throws Exception {
            // 判断另一条流中对应事件是否到来
            OrderEvent orderPay = orderPayState.value();
            if ( orderPay != null){
                // 如果已经到来，匹配成功，直接输出信息，清空状态
                out.collect("订单" + ctx.getCurrentKey() + "对账成功！" + orderPay + " -> " + thirdpartyPay);
                orderPayState.clear();
            } else {
                // 如果没来，注册定时器开始等待，保存状态
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000L);
                thirdpartyPayState.update(thirdpartyPay);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedCoProcessFunction<String, OrderEvent, ThirdpartyPayEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            OutputTag<String> outputTag = new OutputTag<String>("warning") {};

            // 定时器触发，判断哪个事件没到，输出报警信息
            if (orderPayState.value() != null)
                ctx.output(outputTag, "订单" + ctx.getCurrentKey() + " 第三方支付平台到账事件未到！");
            if (thirdpartyPayState.value() != null)
                ctx.output(outputTag, "订单" + ctx.getCurrentKey() + " 订单支付事件未到！");

            // 清空状态
            orderPayState.clear();
            thirdpartyPayState.clear();
        }
    }
}
