package com.cn.wifiproject.spark.streaming.kafka

import com.cn.wifiproject.spark.common.SparkConfFactory
import kafka.serializer.StringDecoder
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka.KafkaCluster

/**
  * description:
  * author: Rock
  * create: 2019-07-15 13:02
  **/
object SparkstreamingKafkaTest extends Serializable with Logging{

  val topic = "ch1_test1"

  def main(args: Array[String]): Unit = {


    //获取streamingContext参数
    val ssc = SparkConfFactory.newSparkLocalStreamingContext("SparkstreamingKafkaTest",
                                                              10L,
                                                             1)

    //获取kafka配置信息
    val kafkaParams = getKafkaParam(topic,"consumer")

    //从kafka中获取DS流
    val kafkaDS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))

    kafkaDS.foreachRDD(rdd=>{
      //获取rdd中保存的 kafka offsets信息
      val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetsList.foreach(x=>{
        println("获取rdd中的偏移信息" + x)
      })
      rdd.foreach(x=>{
        x==""
        //println(x)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }



  /**
   * @Description: 获取kafka配置信息
   * @param: [kafkaTopic, groupId]
   * @return: scala.collection.immutable.Map<java.lang.String,java.lang.String>
   * @auther: Rock
   * @date: 2019-07-15 14:14
   */
  def getKafkaParam(kafkaTopic:String,groupId : String): Map[String,String]={
    val kafkaParam=Map[String,String](
      "metadata.broker.list" -> "hadoop-4:9092",
      "auto.offset.reset" -> "smallest",
      "group.id" -> groupId,
      "refresh.leader.backoff.ms" -> "1000",
      "num.consumer.fetchers" -> "8")
    kafkaParam
  }
}
