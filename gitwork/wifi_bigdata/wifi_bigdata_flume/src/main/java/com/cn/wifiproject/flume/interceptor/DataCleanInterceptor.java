package com.cn.wifiproject.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.cn.wifiproject.flume.fields.MapFields;
import com.cn.wifiproject.flume.service.DataCheck;
import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Description: 自定义拦截器 直接实现Interceptor接口
 * @version 1.0
 * @date 2016/9/21 15:24数据清洗过滤器
 */

public class DataCleanInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(DataCleanInterceptor.class);

    @Override
    public void initialize() {
    }


    /**
     * 单条处理
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        SimpleEvent eventNew = new SimpleEvent();
        try {
            LOG.info("拦截器Event开始执行");
            //解析event中的数据，获取正确无误的数据
            Map<String, String> map = parseEvent(event);
            if(map == null){
                return null;
            }
            String lineJson = JSON.toJSONString(map);
            LOG.info("拦截器推送数据到channel:" +lineJson);
            eventNew.setBody(lineJson.getBytes());
        } catch (Throwable t) {
            if (t instanceof Error) {
                throw (Error)t;
            }
            LOG.error("推送数据到channel失败",t);
        }
        return eventNew;
    }


    /**
     * 批处理
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> list = new ArrayList<Event>();
        for (Event event : events) {
            Event intercept = intercept(event);
            if (intercept != null) {
                list.add(intercept);
            }
        }
        return list;
    }

    @Override
    public void close() {
    }

    /**
     * @Description: 获取event数据，并校验数据，返回正确的map和保存错误数据的Map到ES
     * @param: [event]
     * @return: java.util.Map<java.lang.String,java.lang.String>
     * @auther: Rock
     * @date: 2019-07-17 12:17
     */
    public static Map<String,String> parseEvent(Event event){
        if (event == null) {
            return null;
        }
        // 000000000000000	000000000000000	23.000000	24.000000	aa-aa-aa-aa-aa-aa	bb-bb-bb-bb-bb-bb	32109231	1557305988	andiy	18609765432	judy			1789098762
        String line = new String(event.getBody(), Charsets.UTF_8);
        String filename = event.getHeaders().get(MapFields.FILENAME);
        String absoluteFilename = event.getHeaders().get(MapFields.ABSOLUTE_FILENAME);
        // String转map，并进行数据校验，校验错误入ES错误表
        Map<String, String> map = DataCheck.txtParseAndalidation(line,filename,absoluteFilename);
        return map;
    }

    /**
     * @Description: 使用拦截器必须要创建Builder类实现Interceptor.Builder
     *               对应flume.conf 中的拦截器配置
     * @param:
     * @return:
     * @auther: Rock
     * @date: 2019-07-17 11:05
     */
    public static class Builder implements Interceptor.Builder {
        @Override
        public void configure(Context context) {
        }
        @Override
        public Interceptor build() {
            return new DataCleanInterceptor();
        }
    }
}
