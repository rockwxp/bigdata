package com.cn.wifiproject.spark.streaming.kafka

import com.cn.wifiproject.spark.common.SparkConfFactory
import kafka.serializer.StringDecoder
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}


/**
  * description:
  * author: Rock
  * create: 2019-07-15 13:02
  **/
object SparkstreamingKafkaTest extends Serializable with Logging{

  val topic = "chl_test11"
  def main(args: Array[String]): Unit = {

    //获取streamingContext参数
    val ssc = SparkConfFactory.newSparkLocalStreamingContext("SparkstreamingKafkaTest",
      10L,
      1)
    //获取kafka配置信息
    val kafkaParams = getKafkaParam(topic,"consumer2")
    //从kafka中获取DS流
    val kafkaDS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))
    //(null,{"rksj":"1558178497","latitude":"24.000000","imsi":"000000000000000"})
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
    * 获取kafkaParams
    * @param kafkaTopic
    * @param groupId
    * @return
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
