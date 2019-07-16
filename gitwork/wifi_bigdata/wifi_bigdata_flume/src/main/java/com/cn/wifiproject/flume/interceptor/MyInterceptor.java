package com.cn.wifiproject.flume.interceptor;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * description:
 * author: Rock
 * create: 2019-07-16 11:41
 **/
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        events.forEach(event -> {
            intercept(event);
        });
        return null;
    }

    @Override
    public void close() {

    }
}
