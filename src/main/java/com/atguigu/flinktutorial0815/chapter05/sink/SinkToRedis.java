package com.atguigu.flinktutorial0815.chapter05.sink;

import com.atguigu.flinktutorial0815.chapter05.ClickEvent;
import com.atguigu.flinktutorial0815.chapter05.ClickEventSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class SinkToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<ClickEvent> input = env.addSource(new ClickEventSource());

        // 创建Jedis连接配置项
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        // 创建RedisMapper，指明写入数据的命令
        RedisMapper<ClickEvent> mapper = new RedisMapper<ClickEvent>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "clicks");
            }

            @Override
            public String getKeyFromData(ClickEvent data) {
                return data.user;
            }

            @Override
            public String getValueFromData(ClickEvent data) {
                return data.url + ", " + data.ts;
            }
        };

        input.addSink( new RedisSink<>(config, mapper));

        env.execute();
    }
}
