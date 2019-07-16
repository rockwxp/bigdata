package com.cn.wifiproject.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * description:
 * author: Rock
 * create: 2019-07-16 09:49
 **/
public class MySource extends AbstractSource implements Configurable, PollableSource {
    private String myProp;

    /**
     * @Description: 读取flume.conf配置文件
     * @param: [context]
     * @return: void
     * @auther: Rock
     * @date: 2019-07-16 09:54
     */
    @Override
    public void configure(Context context) {

        //参数获取
        String myProp = context.getString("myProp", "defaultValue");

        // Process the myProp value (e.g. validation, convert to another type, ...)

        // Store myProp for later retrieval by process() method
        this.myProp = myProp;
    }



    /**
     * @Description: 核心 定义Source自己的处理逻辑
     * @param: []
     * @return: org.apache.flume.PollableSource.Status
     * @auther: Rock
     * @date: 2019-07-16 10:50
     */
    @Override
    public Status process() throws EventDeliveryException {

        //事件处理状态
        Status status = null;

        try {
            // This try clause includes whatever Channel/Event operations you want to do

            // Receive new data 数据封装成event进行传输
            Event e = new SimpleEvent();

            // Store the Event into this Source's associated Channel(s)
            getChannelProcessor().processEvent(e);

            status = Status.READY;
        } catch (Throwable t) {
            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {

        }
        return status;
    }

    @Override
    public void start() {
        // Initialize the connection to the external client
    }

    @Override
    public void stop () {
        // Disconnect from external client and do any additional cleanup
        // (e.g. releasing resources or nulling-out field values) ..
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
