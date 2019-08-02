package com.rock.streaming

import scala.collection.mutable.ArrayBuffer

/**
  * description:
  * author: Rock
  * create: 2019-08-02 11:11
  **/
object testCode {
  def main(args: Array[String]): Unit = {
    val score = ArrayBuffer[(Int, Double)]()

    println(math.log(3)/math.log(2))

    for(x <- 1 to 3; y <- 1 to 3){
      score += ((x,y))
      //println(score)
      //score.groupBy(_._1).foreach(println)


    }

    println(score)
    val a =score.groupBy(_._1).map {
      case (mid,sims) =>
        println(sims.map(_._2).sum)
        println(sims.length)
        (mid,sims.map(_._2).sum / sims.length)
    }.toArray

    a.foreach(println)
  }


}
