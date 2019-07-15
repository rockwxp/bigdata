package com.cn.wifiproject.spark.common


import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}

/**
  * @description:
  * @author: Rock
  * @create: 2019-07-15 12:32
  **/
object SparkConfFactory extends Serializable with Logging {

  /**
   * @Description: 获取sparkConf
   * @param: [appName, threads]
   * @return: org.apache.spark.SparkConf
   * @auther: Rock
   * @date: 2019-07-15 12:43
   */
  def newSparkLocalConf(appName: String = "spark local", threads: Int = 1): SparkConf = {
    return new SparkConf().setMaster(s"local[$threads]").setAppName(appName)
  }





  def newSparkLocalStreamingContext(appName: String = "sparkstreaming",
                                    batchInternval:Long=30L,
                                    threads: Int = 1):StreamingContext ={

    val sparkConf = SparkConfFactory.newSparkLocalConf(appName,threads)

    return new StreamingContext(sparkConf,Seconds(batchInternval))



  }


}
