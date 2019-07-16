package org.apache.spark.streaming.kafka

import com.cn.wifiproject.spark.streaming.kafka.SparkstreamingKafkaTest.getKafkaParam
import org.apache.spark.Logging

/**
  * description:
  * author: Rock
  * create: 2019-07-15 16:16
  **/
object KafkaClusterTest extends Serializable with Logging {


  val topic = "ch1_test2";

  val kafkaParams = getKafkaParam(topic, "consumer2")

  //获取kafkaCluster @transient防止并发
  @transient
  private var cluster = new KafkaCluster(kafkaParams)

  //定义一个单例
  def kc(): KafkaCluster = {
    if (cluster == null) {
      cluster = new KafkaCluster(kafkaParams)
    }
    cluster
  }

  def main(args: Array[String]): Unit = {
    //获取消费者组名
    val groupId = "consumer";
    //获取kafka本身的偏移量
    val partitionE = kc.getPartitions(Set(topic))
    require(partitionE.isRight,s"获取Partition失败")
    println("partition=="+partitionE)

    val partitions = partitionE.right.get
    println("打印分区信息")
    partitions.foreach(println(_))

    //获取kafka昨早的offsets
    val earliestLeader = kc.getEarliestLeaderOffsets(partitions)
    require(earliestLeader.isRight,s"获取earliestLeader失败")
    val earliestLeaderOffset = earliestLeader.right.get
    println("kafka最首端偏移信息")
    earliestLeaderOffset.foreach(println(_))

    //获取kafka最末尾的offsets
    val latestLeader = kc.getLatestLeaderOffsets(partitions)
    require(latestLeader.isRight,s"获取earliestLeader失败")
    val latestLeaderOffset = latestLeader.right.get
    println("kafka最末端偏移信息")
    latestLeaderOffset.foreach(println(_))
   
    //获取消费者的offsets
    val consumerOffsetsE = kc.getConsumerOffsets(groupId,partitions)
    require(consumerOffsetsE.isRight,s"获取consumerOffsets失败")
    val consumerOffsets = consumerOffsetsE.right.get
    println("consumer偏移信息")
    consumerOffsets.foreach(println(_))


  }


}
