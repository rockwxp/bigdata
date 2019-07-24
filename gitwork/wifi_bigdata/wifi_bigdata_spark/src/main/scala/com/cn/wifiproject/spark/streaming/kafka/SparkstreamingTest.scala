package com.cn.wifiproject.spark.streaming.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * description:
  * author: Rock
  * create: 2019-07-22 10:38
  **/
object SparkstreamingTest {


  def main(args: Array[String]): Unit = {

    //1.创建Conf
    val conf = new SparkConf().setAppName("sparkstreamingTest").setMaster("local[2]")
    println("创建Conf成功")
    //2。通过conf，设置采样的时间间隔 获取StreamingContext
    val ssc = new StreamingContext(conf,Seconds(3))

    //获取DStream（RDD）接收数据方式：从netcat服务器上接收数据
    val lines = ssc.socketTextStream("bigdata111",1234,StorageLevel.MEMORY_ONLY)

    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map((_,1)).reduceByKey(_+_)


    wordCount.print()
    //启动StreamingContext
    ssc.start()
    ssc.awaitTermination()




  }

}
