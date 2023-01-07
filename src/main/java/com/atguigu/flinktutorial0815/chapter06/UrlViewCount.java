package com.atguigu.flinktutorial0815.chapter06;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class UrlViewCount {
    public String url;
    public Long count;
    public Long start;
    public Long end;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long count, Long start, Long end) {
        this.url = url;
        this.count = count;
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
