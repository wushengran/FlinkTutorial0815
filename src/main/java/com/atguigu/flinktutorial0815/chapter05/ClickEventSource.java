package com.atguigu.flinktutorial0815.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0815
 * <p>
 * Created by  wushengran
 */

public class ClickEventSource implements SourceFunction<ClickEvent> {
    // 定义一个标志位，用来表示当前的运行状态
    private boolean flag = true;

    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        Random random = new Random();

        // 定义数据随机选择的集合范围
        String[] users = {"Alice", "Bob", "Cary", "Mary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (flag){
            // 随机生成数据
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long ts = Calendar.getInstance().getTimeInMillis();

            ctx.collect(new ClickEvent(user, url, ts));
//            ctx.collectWithTimestamp(new ClickEvent(user, url, ts), ts);
//            ctx.emitWatermark(new Watermark(ts));

            // 暂停1s
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
