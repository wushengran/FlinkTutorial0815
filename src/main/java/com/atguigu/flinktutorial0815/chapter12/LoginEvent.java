package com.atguigu.flinktutorial0815.chapter12;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class LoginEvent {
    public String user;
    public String ip;
    public String status;
    public Long ts;

    public LoginEvent() {
    }

    public LoginEvent(String user, String ip, String status, Long ts) {
        this.user = user;
        this.ip = ip;
        this.status = status;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "user='" + user + '\'' +
                ", ip='" + ip + '\'' +
                ", status='" + status + '\'' +
                ", ts=" + ts +
                '}';
    }
}
