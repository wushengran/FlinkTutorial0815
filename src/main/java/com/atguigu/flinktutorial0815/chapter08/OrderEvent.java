package com.atguigu.flinktutorial0815.chapter08;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class OrderEvent {
    public String orderId;
    public String userId;
    public String status;
    public Long ts;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String userId, String status, Long ts) {
        this.orderId = orderId;
        this.userId = userId;
        this.status = status;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", status='" + status + '\'' +
                ", ts=" + ts +
                '}';
    }
}
