package com.cn.wifiproject.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-03-07 10:34
 */
public class ConfigUtil {

    private static Logger LOG = LoggerFactory.getLogger(ConfigUtil.class);

    private static ConfigUtil configUtil;

    public static ConfigUtil getInstance(){

        if(configUtil == null){
            configUtil = new ConfigUtil();
        }
        return configUtil;
    }


    public Properties getProperties(String path){
        Properties properties = new Properties();
        try {
            LOG.info("开始加载配置文件" + path);
            InputStream insss = this.getClass().getClassLoader().getResourceAsStream(path);
            properties = new Properties();
            properties.load(insss);
        } catch (IOException e) {
            LOG.info("加载配置文件" + path + "失败");
            LOG.error(null,e);
        }

        LOG.info("加载配置文件" + path + "成功");
        System.out.println("文件内容："+properties);
        return properties;
    }


    public static void main(String[] args) {
        ConfigUtil instance = ConfigUtil.getInstance();
        Properties properties = instance.getProperties("common/datatype.properties");
        System.out.println(properties);
    }
}
