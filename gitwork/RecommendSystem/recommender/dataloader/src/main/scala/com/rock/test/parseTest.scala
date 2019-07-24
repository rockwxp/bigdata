package com.rock.test

/**
  * description:
  * author: Rock
  * create: 2019-07-23 21:51
  **/
object parseTest {

  def main(args: Array[String]): Unit = {
    val str ="1^Toy Story (1995)^ ^81 minutes^March 20, 2001^1995^English ^Adventure"
      val strings = str.split("\\^")
      strings.foreach(println)

  }
}
