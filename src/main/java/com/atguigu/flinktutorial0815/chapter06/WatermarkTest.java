package com.atguigu.flinktutorial0815.chapter06;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100L);


        DataStreamSource<ClickEvent> input = env.addSource(new ClickEventSource());

        // 自定义水位线生成策略
        input.assignTimestampsAndWatermarks(
                        new WatermarkStrategy<ClickEvent>() {
                            @Override
                            public WatermarkGenerator<ClickEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new MyWatermarkGenerator(2000L);
                            }

                            @Override
                            public TimestampAssigner<ClickEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new SerializableTimestampAssigner<ClickEvent>() {
                                    @Override
                                    public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                };
                            }
                        }

                );

        // 内置水位线生成策略
        // 处理乱序数据
        input.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner( (element, recordTimestamp) -> element.ts )
        );

        // 处理有序数据
        input.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner( (element, recordTimestamp) -> element.ts )
        );

        env.execute();
    }

    public static class MyWatermarkGenerator implements WatermarkGenerator<ClickEvent> {
        private long delay;
        private long maxTs = Long.MIN_VALUE + delay + 1;

        public MyWatermarkGenerator(long delay) {
            this.delay = delay;
        }

        @Override
        public void onEvent(ClickEvent event, long eventTimestamp, WatermarkOutput output) {
            // 每来一个数据，调用这个方法，直接按照当前数据时间戳发出水位线
//            output.emitWatermark(new Watermark(eventTimestamp));

            // 保存当前最大时间戳
            maxTs = Math.max(maxTs, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 周期性发出水位线
            output.emitWatermark(new Watermark(maxTs - delay - 1));
        }
    }
}
