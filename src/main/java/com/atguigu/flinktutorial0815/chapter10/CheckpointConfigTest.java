package com.atguigu.flinktutorial0815.chapter10;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class CheckpointConfigTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(300);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://...");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 30000L));
//        env.setDefaultSavepointDirectory("");

        // 定义检查点配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointInterval(200);
        checkpointConfig.setCheckpointTimeout(60 * 1000L);     // 设置检查点超时时间
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMaxConcurrentCheckpoints(2);    // 设置同时保存的检查点最大个数
        checkpointConfig.setMinPauseBetweenCheckpoints(500);     // 设置检查点间最小暂停时间
        checkpointConfig.setTolerableCheckpointFailureNumber(3);     // 设置检查点允许失败次数
        checkpointConfig.enableUnalignedCheckpoints();     // 开启非对齐的检查点保存方式
        checkpointConfig.setAlignmentTimeout(Duration.ofSeconds(1));    // 设置对齐的等待超时时间
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.execute();
    }
}
