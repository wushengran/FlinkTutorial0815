package com.atguigu.flinktutorial0815.chapter08;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class ThirdpartyPayEvent {
    public String orderId;
    public Double amount;
    public String platform;
    public Long ts;

    public ThirdpartyPayEvent() {
    }

    public ThirdpartyPayEvent(String orderId, Double amount, String platform, Long ts) {
        this.orderId = orderId;
        this.amount = amount;
        this.platform = platform;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ThirdpartyPayEvent{" +
                "orderId='" + orderId + '\'' +
                ", amount=" + amount +
                ", platform='" + platform + '\'' +
                ", ts=" + ts +
                '}';
    }
}
