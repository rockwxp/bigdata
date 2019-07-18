package com.cn.wifiproject.flume.sink;


import com.cn.wifiproject.kafka.producer.StringProducer;
import com.google.common.base.Throwables;
import kafka.producer.KeyedMessage;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;


public class KafkaSink extends AbstractSink implements Configurable {

	private final Logger logger = Logger.getLogger(KafkaSink.class);
	private String[] kafkatopics = null;
	private List<KeyedMessage<String,String>> listKeyedMessage=null;
	private Long proTimestamp=System.currentTimeMillis();

	@Override
	public void configure(Context context) {
		kafkatopics = context.getString("kafkatopics").split(",");
		logger.info("获取kafka topic配置" + context.getString("kafkatopics"));
		listKeyedMessage=new ArrayList<>();
	}

	@Override
	public Status process() throws EventDeliveryException {

		logger.info("sink开始执行");
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		try {
			Event event = channel.take();
			if (event == null) {
				transaction.rollback();
				return Status.BACKOFF;
			}
			// 解析记录
			String recourd = new String(event.getBody());
			// 发送数据到kafka
			try {

				logger.info("发送到kafka的数据： topic："+kafkatopics[0]+" event.body:"+recourd);
				StringProducer.producer(kafkatopics[0],recourd);

			/*	if(listKeyedMessage.size()>1000){
					logger.info("数据大与10000,推送数据到kafka");
					sendListKeyedMessage();
					logger.info("数据大与10000,推送数据到kafka成功");
				}else if(System.currentTimeMillis()-proTimestamp>=60*1000){
					logger.info("时间间隔大与60,推送数据到kafka");
					sendListKeyedMessage();
					logger.info("时间间隔大与60,推送数据到kafka成功"+listKeyedMessage.size());
				}*/

			} catch (Exception e) {
				logger.error("推送数据到kafka失败" , e);
				throw Throwables.propagate(e);
			}

			transaction.commit();
			return Status.READY;
		} catch (ChannelException e) {
			logger.error(e);
			transaction.rollback();
			return Status.BACKOFF;
		} finally {
			if(transaction != null){
				transaction.close();
			}
		}
	}

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}


	/*private void sendListKeyedMessage(){
		Producer<String, String> producer = new Producer<>(KafkaConfig.getInstance().getProducerConfig());
		producer.send(listKeyedMessage);
		listKeyedMessage.clear();
		proTimestamp=System.currentTimeMillis();
		producer.close();
	}*/


}

