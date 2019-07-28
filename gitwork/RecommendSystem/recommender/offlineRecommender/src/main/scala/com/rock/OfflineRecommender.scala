package com.rock

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession


/**
  * description: 离线推荐算法实现
  * author: Rock
  * create: 2019-07-28 09:57
  **/
object OfflineRecommender {



  val MONGODB_RATING_COLLECTION="Rating"
  val MONGODB_MOVIE_COLLECTION="Movie"

  val MONGODB_USER_RECS="UserRecs"
  val USER_MAX_RECOMMENDATION=10


  def main(args: Array[String]): Unit = {

    val conf = Map(
      "spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://bigdata113:27017/recom",
      "mongo.db" -> "recom"
    )

    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster("local[2]")
      .set("spark.executor.memory","6G")
      .set("spark.driver.memory","2G")//给客户设置

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(conf("mongo.uri"),conf("mongo.db"))

    import spark.implicits._

    //获取评分表数据转Rdd，保留字段：用户ID，电影ID，电影评分
    val ratingRdd = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating].rdd.map(rating => (rating.uid,rating.mid,rating.score)).cache()

    //读取movie表转RDD，只要电影ID
    val movieRdd = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie].rdd.map(_.mid).cache()



    //训练ALS模型
    /**
      * 传入4个参数：
      *
      * trainData
      * 训练数据
      * Rating对象的集合，包含：用户ID、物品ID、偏好值
      *
      * rank
      * 特征维度：50
      *
      * iterations
      * 迭代次数：5 过多导致过拟合状态
      *
      * lambda：//损失函数，减少误差，越精确，计算时间越长
      *
      * 0.01
      *
      */
    //把训练数据导入spark.mllib中导Rating对象中
    val trainData = ratingRdd.map(x => Rating(x._1,x._2,x._3))

    val (rank,iterations,lambda) = (50,5,0.01)

    //把参数放入ALS，生产算法模型
    val model = ALS.train(trainData,rank,iterations,lambda)
    //计算用户推荐矩阵
    //获取所有User的id 转RDD
    val uIDRdd = ratingRdd.map(_._1).distinct().cache()

    //笛卡尔积，将每个电影都ID都关联每个用户
    val userMovie = uIDRdd.cartesian(movieRdd)

    //把数据放入模型生成预测结果
    val preRating = model.predict(userMovie)

    //开始整理预测结果
    val userRecs =preRating
      .filter(_.rating >0 )
      .map(rating => (rating.user,(rating.product,rating.rating)))
      .groupByKey()
      .map{
        case (uid,movies) =>{
          UserRecs(uid,movies.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x=>Recommendation(x._1,x._2)))
        }
      }.toDF

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingRdd.unpersist()
    movieRdd.unpersist()
    uIDRdd.unpersist()
    spark.close()



  }

}
