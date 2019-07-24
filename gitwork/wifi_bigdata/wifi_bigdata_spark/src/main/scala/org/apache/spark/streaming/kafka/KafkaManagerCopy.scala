package org.apache.spark.streaming.kafka

import org.apache.spark.Logging

/**
  * author: KING
  * description:
  * Date:Created in 2019-05-23 20:07
  */
class KafkaManagerCopy(val kafkaParams: Map[String, String],
                       val autoUpdateoffset: Boolean = true) extends Serializable with Logging {

  @transient
  private var cluster = new KafkaCluster(kafkaParams)

  def kc(): KafkaCluster = {
    if (cluster == null) {
      cluster = new KafkaCluster(kafkaParams);
    }
    cluster
  }

/*

  def createDirectStream[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K] : ClassTag,
  VD <: Decoder[V] : ClassTag](ssc: StreamingContext,
                               topics: Set[String]
                              ): Unit = {

    //获取消费者组ID
    val groupId = kafkaParams.get("group.id")
    //先获取kafka offset信息
    //获取kafka本身的偏移量  Either类型可以认为就是封装了2钟信息
    val partitionsE = kc.getPartitions(Set(topic))
    require(partitionsE.isRight, s"获取partion失败")
    println("partitionsE==" + partitionsE)
    val partitions = partitionsE.right.get
    println("打印分区信息")
    partitions.foreach(println(_))

    //获取kafka最早的offsets
    val earliestLeader = kc.getEarliestLeaderOffsets(partitions)
    require(earliestLeader.isRight, s"获取earliestLeader失败")
    val earliestLeaderOffsets = earliestLeader.right.get
    println("kafka最早的偏移量")
    earliestLeaderOffsets.foreach(println(_))


    //获取kafka最末尾的offsets
    val latestLeader = kc.getLatestLeaderOffsets(partitions)
    require(latestLeader.isRight, s"获取latestLeader失败")
    val latestLeaderOffsets = latestLeader.right.get
    println("kafka最末尾的偏移量")
    latestLeaderOffsets.foreach(println(_))

    //获取消费者的offsets
    kc.getConsumerOffsets()


    /*   val ConsumerOffsetE = kc.getConsumerOffsets(groupId,partitions)
       require(ConsumerOffsetE.isRight,s"获取ConsumerOffsetE失败")
       val consumerOffsets = ConsumerOffsetE.right.get
       println("打印消费者偏移量")
       consumerOffsets.foreach(println(_))
   */

    //获取消费者组信息


    //根据消费者组的offsets去消费


  }
*/


}
