package com.atguigu.flinktutorial0815.chapter09;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class KeyedStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<ClickEvent> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.ts)
                );

//        stream.keyBy(value -> value.user)
//                .map(new RichMapFunction<ClickEvent, String>() {
//                    private ValueState<Long> countState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
//                    }
//
//                    @Override
//                    public String map(ClickEvent value) throws Exception {
//                        if (countState.value() == null)
//                            countState.update(1L);
//                        else
//                            countState.update(countState.value() + 1);
//                        return "用户" + value.user + "的访问频次为：" + countState.value();
//                    }
//                })
//                .print();

        // 测试各种类型的KeyedState
        stream.keyBy(value -> value.user)
                .flatMap(new RichFlatMapFunction<ClickEvent, String>(){
                    // 声明状态
                    private ValueState<Long> myValueState;
                    private ListState<ClickEvent> myListState;
                    private MapState<String, Long> myMapState;

                    private ReducingState<ClickEvent> myReducingState;
                    private AggregatingState<ClickEvent, String> myAggregatingState;

                    // 定义一个属性
                    private Long count = 0L;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("my-value", Types.LONG);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                                .build();
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        myValueState = getRuntimeContext().getState(valueStateDescriptor);

                        myListState = getRuntimeContext().getListState(new ListStateDescriptor<ClickEvent>("my-list", ClickEvent.class));
                        myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", Types.STRING, Types.LONG));

                        ReducingStateDescriptor<ClickEvent> reducingStateDescriptor = new ReducingStateDescriptor<ClickEvent>(
                                "my-reducing",
                                (value1, value2) -> new ClickEvent(value1.user, value1.url + ", " + value2.url, value2.ts),
                                ClickEvent.class
                        );
                        myReducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);

                        AggregatingStateDescriptor<ClickEvent, Long, String> aggregatingStateDescriptor = new AggregatingStateDescriptor<ClickEvent, Long, String>(
                                "my-aggregating",
                                new AggregateFunction<ClickEvent, Long, String>() {
                                    @Override
                                    public Long createAccumulator() {
                                        return 0L;
                                    }

                                    @Override
                                    public Long add(ClickEvent value, Long accumulator) {
                                        return accumulator + 1;
                                    }

                                    @Override
                                    public String getResult(Long accumulator) {
                                        return "当前聚合状态为：" + accumulator;
                                    }

                                    @Override
                                    public Long merge(Long a, Long b) {
                                        return a + b;
                                    }
                                },
                                Long.class
                        );

                        myAggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
                    }

                    @Override
                    public void flatMap(ClickEvent value, Collector<String> out) throws Exception {
                        out.collect("----------------------------");
                        out.collect(value.user);
                        out.collect(value.url + ", " + value.ts);

                        // 使用值状态
                        if (myValueState.value() == null)
                            myValueState.update(1L);
                        else
                            myValueState.update(myValueState.value() + 1);
                        out.collect("值状态为：" + myValueState.value());

                        // 使用列表状态
                        myListState.add(value);
                        out.collect("列表状态为：" + myListState.get());

                        // 使用映射状态，统计每个用户对每个url的访问频次
                        if (myMapState.contains(value.url))
                            myMapState.put(value.url, myMapState.get(value.url) + 1);
                        else
                            myMapState.put(value.url, 1L);
                        out.collect("映射状态中" + value.url + "的点击量为：" + myMapState.get(value.url));

                        // 使用归约状态
                        myReducingState.add(value);
                        out.collect("归约状态为：" + myReducingState.get());

                        // 使用聚合状态，统计每个用户的访问频次
                        myAggregatingState.add(value);
                        out.collect(myAggregatingState.get());

                        // 作为对比，将属性加1
                        count ++;
                        out.collect("属性count值为：" + count);
                        out.collect("当前分区索引号为：" + getRuntimeContext().getIndexOfThisSubtask());
                    }
                })
                .print();

        env.execute();
    }

    public static class MyProcess extends RichFlatMapFunction<ClickEvent, String>
    implements CheckpointedFunction{
        @Override
        public void flatMap(ClickEvent value, Collector<String> out) throws Exception {

        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            if (context.isRestored()){
//                context.getOperatorStateStore().getListState()
            }
        }
    }
}
