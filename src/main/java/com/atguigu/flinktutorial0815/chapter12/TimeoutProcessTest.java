package com.atguigu.flinktutorial0815.chapter12;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.zookeeper.Login;

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

public class TimeoutProcessTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据源
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((element, recordTimestamp) -> element.ts)
        );

        // 2. 定义匹配规则
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first-fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.status.equals("fail");
                    }
                })
                .next("second-fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.status.equals("fail");
                    }
                })
                .next("third-fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.status.equals("fail");
                    }
                })
                .within(Time.seconds(10));

        // 3. 将模式应用到数据流上，检测匹配的复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(value -> value.user), pattern);

        // 4. 定义一个输出标签，用来表示超时部分匹配事件的侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };

        // 5. 对检测到的复杂事件进行处理转换，输出报警信息
        SingleOutputStreamOperator<String> warningStream = patternStream.select(
                outputTag,
                new MyTimeout(),
                new MySelect()
        );

        SingleOutputStreamOperator<String> warningStream2 = patternStream.process(new MyProcess());

        // 6. 捕获侧输出流输出
        warningStream2.getSideOutput(outputTag).print("timeout");

        warningStream2.print();

        env.execute();
    }

    // 实现自定义的PatternTimeoutFunction
    public static class MyTimeout implements PatternTimeoutFunction<LoginEvent, String>{
        @Override
        public String timeout(Map<String, List<LoginEvent>> pattern, long timeoutTimestamp) throws Exception {
            LoginEvent first = pattern.get("first-fail").get(0);
            return "用户 " + first.user + " 在10s内登录失败未达3次！超时时间为：" + timeoutTimestamp;
        }
    }

    // 实现自定义的PatternSelectFunction
    public static class MySelect implements PatternSelectFunction<LoginEvent, String> {
        @Override
        public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
            LoginEvent first = pattern.get("first-fail").get(0);
            LoginEvent second = pattern.get("second-fail").get(0);
            LoginEvent third = pattern.get("third-fail").get(0);
            return "用户 " + first.user + " 在10s内连续三次登录失败！登录时间为：" +
                    first.ts + ", " + second.ts + ", " + third.ts;
        }
    }

    // 实现自定义的PatternProcessFunction
    public static class MyProcess extends PatternProcessFunction<LoginEvent, String>
       implements TimedOutPartialMatchHandler<LoginEvent> {
        @Override
        public void processMatch(Map<String, List<LoginEvent>> pattern, Context ctx, Collector<String> out) throws Exception {
            LoginEvent first = pattern.get("first-fail").get(0);
            LoginEvent second = pattern.get("second-fail").get(0);
            LoginEvent third = pattern.get("third-fail").get(0);
            out.collect("用户 " + first.user + " 在10s内连续三次登录失败！登录时间为：" +
                    first.ts + ", " + second.ts + ", " + third.ts);
        }

        @Override
        public void processTimedOutMatch(Map<String, List<LoginEvent>> match, Context ctx) throws Exception {
            LoginEvent first = match.get("first-fail").get(0);
            ctx.output(new OutputTag<String>("timeout") {},
                    "用户 " + first.user + " 在10s内登录失败未达3次！超时时间为：" + ctx.timestamp());
        }
    }
}
