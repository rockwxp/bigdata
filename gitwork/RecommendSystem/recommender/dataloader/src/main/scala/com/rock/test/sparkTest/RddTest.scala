package com.rock.test.sparkTest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * description:
  * author: Rock
  * create: 2019-07-26 14:26
  **/
object RddTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RddTest").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("Tom",1),("Tom",2),("jerry",1),("Mike",2)))
    val rdd2 = sc.parallelize(List(("jerry",2),("Tom",1),("Bob",2)))
    val rdd3 = rdd1.cogroup(rdd2)

    rdd3.foreach(println)
  }

}
