package com.rock

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * description: 训练模型最优参数 更好对使用ALS
  * Alternating least squares
  * author: Rock
  * create: 2019-07-28 09:58
  **/
object ALSTrainner {


  def main(args: Array[String]): Unit = {

    val conf = Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://bigdata113:27017/recom",
      "mongo.db" -> "recom"
    )

    //创建Spark Conf
    val sparkConf = new SparkConf().setAppName("ALS Trainner").setMaster(conf("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //加载评分数据
    val mongoConfig = MongoConfig(conf("mongo.uri"),conf("mongo.db"))
    import spark.implicits._

    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",OfflineRecommender.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid,rating.mid,rating.score)).cache()

    //输出最优参数
    adjustALSParams(ratingRDD)

    spark.close()
  }


  //输出最优参数
  def adjustALSParams(ratingRDD: RDD[Rating]): Unit = {
    val result = for (rank <- Array(30,40,50,60,70); lambda <- Array(1,0.1,0.01))
      yield{
        val model = ALS.train(ratingRDD,rank,5,lambda)
        //获取模型误差
        val rmse = getRmse(model,ratingRDD)
        (rank,lambda,rmse)
      }
    println(result.sortBy(_._3).head)
  }

  def getRmse(model: MatrixFactorizationModel, ratingRDD: RDD[Rating]) = {

    //需要构造 usersProducts RDD
    val userMovies = ratingRDD.map(item => (item.user,item.product))
    val predictRating = model.predict(userMovies)

    val real = ratingRDD.map(item => ((item.user,item.product),item.rating))
    val predict = predictRating.map(item => ((item.user,item.product),item.rating))

    sqrt(
      real.join(predict)//(int,int),(double ,double)
        .map{
        case ((uid,mid),(real,pre)) =>
          //计算真实值和预测值之间的差值
          val err = real - pre
          err * err
      }.mean()
    )
  }

}
