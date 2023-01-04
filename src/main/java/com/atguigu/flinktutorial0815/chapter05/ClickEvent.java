package com.atguigu.flinktutorial0815.chapter05;

import org.apache.kafka.common.protocol.types.Field;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class ClickEvent {
    public String user;
    public String url;
    public Long ts;

    public ClickEvent() {
    }

    public ClickEvent(String user, String url, Long ts) {
        this.user = user;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ClickEvent{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", ts=" + ts +
                '}';
    }
}
