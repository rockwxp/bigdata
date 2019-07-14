package com.cn.wifiproject.kafka.producer;


import com.cn.wifiproject.kafka.config.KafkaConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-05-16 11:18
 */
public class StringProducer {
    private static final Logger LOG = LoggerFactory.getLogger(StringProducer.class);
    private static int threadSize = 6;


    /**
     * 生产单条消息
     * @param topic
     * @param recourd
     */
    public static void producer(String topic,String recourd){
        Producer<String, String> producer = new Producer<>(KafkaConfig.getInstance().getProducerConfig());
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(topic, recourd);
        producer.send(keyedMessage);
        LOG.info("发送数据"+recourd+"到kafka成功");
        producer.close();
    }

    /**
     * 批量
     * @param topic
     * @param listRecourd
     */
    public static void producerList(String topic,List<String> listRecourd){
        Producer<String, String> producer = new Producer<>(KafkaConfig.getInstance().getProducerConfig());
        List<KeyedMessage<String, String>> listKeyedMessage= new ArrayList<>();
        listRecourd.forEach(recourd->{
            listKeyedMessage.add(new KeyedMessage<>(topic, recourd));
        });
        producer.send(listKeyedMessage);
        producer.close();
    }

   /* public static void producer(String topic,List<KeyedMessage<String,String>> listMessage) throws Exception{
        int size = listMessage.size();
        int threads = ( ( size - 1  ) / threadSize ) + 1;

        long t1 = System.currentTimeMillis();
        CountDownLatch cdl = new CountDownLatch(threads);
        //使用线程池
        ExecutorService executorService = ThreadPoolManager.getInstance().getExecutorService();
        LOG.info("开启 " + threads + " 个线程来向  topic " + topic + " 生产数据 . ");
      *//*  for( int i = 0 ; i < threads ; i++ ){
            executorService.execute( new StringProducer.ChildProducer( start , end ,  topic , id,  cdl ));
        }*//*
        cdl.await();
        long t = System.currentTimeMillis() - t1;
        LOG.info(  " 一共耗时  ：" + t + "  毫秒 ... " );
        executorService.shutdown();
    }

    static class ChildProducer implements Runnable{

        public ChildProducer( int start , int end ,  String topic , String id,  CountDownLatch cdl ){


        }

        public void run() {

        }
    }
*/

}
