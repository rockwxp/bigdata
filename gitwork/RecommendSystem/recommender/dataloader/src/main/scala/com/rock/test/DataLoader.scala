package com.rock.test


import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * description:
  * author: Rock
  * create: 2019-07-23 09:48
  **/
object DataLoader {

  //MongoDB 中的表 Collection
  // Moive在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"

  // Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"

  // Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"

  def main(args: Array[String]): Unit = {

    val DATAFILE_MOVIES ="/CentOS/testFile/recommendSystem/small/movies.csv"
    val DATAFILE_RATINGS ="/CentOS/testFile/recommendSystem/small/ratings.csv"
    val DATAFILE_TAGS ="/CentOS/testFile/recommendSystem/small/tags.csv"

    //创建全局配置
    val params = scala.collection.mutable.Map[String,Any]()
    params += "spark.cors" -> "local[2]"
    params += "mongo.uri" -> "mongodb://bigdata113:27017/recom"
    params += "mongo.db"->"recom"

    implicit val mongoConf = new MongoConfig(params("mongo.uri").toString,params("mongo.db").toString)



    //声明spark环境
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(params("spark.cors").toString())

    //获取SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //加载原始数据 转成Rdd
    val movieRdd = spark.sparkContext.textFile(DATAFILE_MOVIES)
    
    val ratingRdd = spark.sparkContext.textFile(DATAFILE_RATINGS)

    val tagRdd = spark.sparkContext.textFile(DATAFILE_TAGS)

    //转换DataFrame
    import spark.implicits._
    var movieDF = movieRdd.map(line=>{
      val x = line.split("\\^")
      Movie(x(0).trim.toInt,x(1).trim,x(2).trim,x(3).trim,x(4).trim,x(5).trim,x(6).trim,
        x(7).trim,x(8).trim,x(9).trim)
    }).toDF()

    var ratingDF = ratingRdd.map(line =>{
      val x = line.split(",")
      Rating(x(0).trim.toInt,x(1).trim.toInt,x(2).trim.toDouble,x(3).toInt)
    })toDF()

    var tagDF = tagRdd.map(line =>{
      val x = line.split(",")
      Tag(x(0).toInt,x(1).toInt,x(2),x(3).toInt)
    })toDF()

    //存入MongoDB
    storeDataInMongo(movieDF,ratingDF,tagDF)

    //引入内置函数库
    import org.apache.spark.sql.functions._
    //将 tagDF 对 movieid 做聚合操作，将tag拼接
    val tagCollectDF = tagDF.groupBy($"mid").agg(concat_ws("|",collect_set($"tag")).as("tags"))
    //将 tags 合并到movie表，产生新的movie数据集
    val esMovieDF = movieDF.join(tagCollectDF,Seq("mid","mid"),"left").select("mid","name","descri","timelong","issue","shoot","language","genres","actors","directors","tags")


    //存入ES
    storeDataInES(esMovieDF)

  }

  /**
   * @Description: 讲数据存入MongoDB
   * @param: [movieDF, ratingDF, tagDF, mongoConfig]
   * @return: scala.Function1<com.rock.test.MongoConfig,scala.runtime.BoxedUnit>
   * @auther: Rock
   * @date: 2019-07-23 14:18
   */
  private def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig:MongoConfig): Unit ={

    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //清空表数据
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).dropCollection();
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).dropCollection();
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).dropCollection();

    //插入表数据
    movieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIES_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATINGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",TAGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //创建索引
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))

    //关闭MongoDB连接
    mongoClient.close()


  }

  private def storeDataInES(esMovieDF: DataFrame): Unit = {

  }

}
