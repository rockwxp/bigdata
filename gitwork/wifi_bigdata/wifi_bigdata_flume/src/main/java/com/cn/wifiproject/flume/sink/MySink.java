package com.cn.wifiproject.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * description:
 * author: Rock
 * create: 2019-07-16 12:21
 **/
public class MySink extends AbstractSink implements Configurable {
    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }

    @Override
    public void configure(Context context) {

    }
}
