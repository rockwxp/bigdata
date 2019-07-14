package com.cn.wifiproject.kafka.config;


import com.cn.wifiproject.common.config.ConfigUtil;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Description:
 * 		kafka配置文件 解析器。
 */

public class KafkaConfig {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaConfig.class);

	private static final String DEFUALT_CONFIG_PATH = "kafka/kafka-server-config.properties";

	private volatile static KafkaConfig kafkaConfig = null;

	private ProducerConfig config;
    private Properties properties;

	private KafkaConfig() throws IOException{

		try {
			properties = ConfigUtil.getInstance().getProperties(DEFUALT_CONFIG_PATH);
		} catch (Exception e) {
			IOException ioException = new IOException();
			ioException.addSuppressed(e);
			throw ioException;
		}

		config = new ProducerConfig(properties);
	}

	public static KafkaConfig getInstance(){
		
		if(kafkaConfig == null){
			synchronized (KafkaConfig.class) {
				if(kafkaConfig == null){
					try {
						kafkaConfig = new KafkaConfig();
					} catch (IOException e) {
						LOG.error("实例化kafkaConfig失败", e);
					}
				}
			}
		}
		return kafkaConfig;
	}
	
    public ProducerConfig getProducerConfig(){
    	return config;
    }
    
    
    /**
      * description:
      * 			
      * 			获取当前时间的字符串   	格式为：	yyyy-MM-dd HH:mm:ss
      * 
      * @return
      *         String
      * 2016-3-28 上午11:33:1
     */
    public static String nowStr(){
    	
    	return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format( new Date() );
    }
    
}
