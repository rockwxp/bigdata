package com.rock.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * description:
  * author: Rock
  * create: 2019-07-26 21:06
  **/
object StatisticsApp extends App {

  val RATINGS_COLLECTION_NAME = "Rating"
  val MOVIE_COLLECTION_NAME = "Movie"

  //创建全局配置
  val params = scala.collection.mutable.Map[String, Any]()
  params += "spark.cores" -> "local[2]"
  params += "mongo.uri" -> "mongodb://bigdata113:27017/recom"
  params += "mongo.db" -> "recom"
  params += "es.httpHosts" -> "bigdata113:9200"
  params += "es.transportHosts" -> "bigdata113:9300"
  params += "es.index" -> "recom" //ES库名
  params += "es.cluster.name" -> "my-application" //ES配置文件elasticsearch.yml cluster.name: my-application

  val conf: SparkConf = new SparkConf().setAppName("StatisticsApp").setMaster("local[2]")

  val spark = SparkSession.builder().config(conf).getOrCreate()

  implicit val mongoConfig = new MongoConfig(params("mongo.uri").toString, params("mongo.db").toString)

  //获取所有历史数据中，评分个数最多的电影集合，统计每个电影评分个数  ->   RateMoreMovies

  //读取mongodb数据
  import spark.implicits._

  val ratings: Dataset[Rating] = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", RATINGS_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating].cache()


  val movies = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", MOVIE_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie].cache()

  ratings.createOrReplaceTempView("ratings")

  //统计电影评分量
 // staticsRecommender.rateMore(spark)
  //按照月统计，这个月中评分最多的电影我们认为是热门电影，统计每个月中的每个电影的评分数量
 //  staticsRecommender.rateMoreRecently(spark)

  staticsRecommender.genresTop10(spark)(movies)

  ratings.unpersist()
  movies.unpersist()

  spark.close()



}
