package com.rock.test


import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.{Setting, Settings}
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


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

  //ES TYPE 名称
  val ES_TAG_TYPE_NAME = "Movie"

  //正则表达式
  val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r


  def main(args: Array[String]): Unit = {

    val DATAFILE_MOVIES ="/CentOS/testFile/recommendSystem/small/movies.csv"
    val DATAFILE_RATINGS ="/CentOS/testFile/recommendSystem/small/ratings.csv"
    val DATAFILE_TAGS ="/CentOS/testFile/recommendSystem/small/tags.csv"

    //创建全局配置
    val params = scala.collection.mutable.Map[String,Any]()
    params += "spark.cores" -> "local[2]"
    params += "mongo.uri" -> "mongodb://bigdata113:27017/recom"
    params += "mongo.db"->"recom"
    params += "es.httpHosts"->"bigdata113:9200"
    params += "es.transportHosts"->"bigdata113:9300"
    params += "es.index"->"recom"//ES库名
    params += "es.cluster.name"->"my-application" //ES配置文件elasticsearch.yml cluster.name: my-application

    //MongoDB的配置隐式参数
    implicit val mongoConf = new MongoConfig(params("mongo.uri").toString,params("mongo.db").toString)

    //ES的配置隐式参数
    implicit val ESConf = new ESConfig(
      params("es.httpHosts").toString,
      params("es.transportHosts").toString,
      params("es.index").toString,
      params("es.cluster.name").toString)


    //声明spark环境
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(params("spark.cores").toString())

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
    //storeDataInMongo(movieDF,ratingDF,tagDF)

    //缓存数据,因为下面再次操作了相应数据
    movieDF.cache()
    tagDF.cache()

    //引入内置函数库
    import org.apache.spark.sql.functions._
    //将 tagDF 对 movieid 做聚合操作，将tag拼接
    val tagCollectDF = tagDF.groupBy($"mid").agg(concat_ws("|",collect_set($"tag")).as("tags"))
    //将 tags 合并到movie表，产生新的movie数据集
    val esMovieDF = movieDF.join(tagCollectDF,Seq("mid","mid"),"left").select("mid","name","descri","timelong","issue","shoot","language","genres","actors","directors","tags")


    //存入ES
    storeDataInES(esMovieDF)

    //清除缓存
    movieDF.unpersist()
    tagDF.unpersist()

    //关闭spark
    spark.close()
  }

  /**
   * @Description: 讲数据存入MongoDB
   * @param: [movieDF, ratingDF, tagDF, mongoConfig]
   * @return: scala.Function1<com.rock.test.MongoConfig,scala.runtime.BoxedUnit>
   * @auther: Rock
   * @date: 2019-07-23 14:18
   */
  private def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig:MongoConfig): Unit ={

    //获取MongoDBClient连接数据
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //清空表数据
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).dropCollection()
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).dropCollection()

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

  /**
   * @Description: 保存数据到ES
   * @param: [esMovieDF, esConfig]
   * @return: scala.Function1<com.rock.test.ESConfig,scala.runtime.BoxedUnit>
   * @auther: Rock
   * @date: 2019-07-25 10:06
   */
  private def storeDataInES(esMovieDF: DataFrame)(implicit esConfig: ESConfig): Unit = {

    //创建ES的Setting 连接ES的配置
    val settings = Settings.builder().put("cluster.name",esConfig.clusterName).build()
    //创建ESClient  新建ES的客户端
    val esClient = new PreBuiltTransportClient(settings)
    //获取ES集群内部每台通讯地址
    esConfig.transportHosts.split(",").foreach{
      //通过正则获取参数，在定义正则中的每个括号，对应下面的每个参数
      case ES_HOST_PORT_REGEX(host:String,port:String) =>
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
    }

    //需要操作的Index名称
    val indexName = esConfig.index

    //判断Index表名是否存在，如果存在就删除表
    if(esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet()isExists){
        //删除Index
      esClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
    }

    //创建表
    esClient.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()

    val movieOptions = Map("es.nodes" -> esConfig.httpHosts,
      "es.http.timeout"->"100m",
      "es.mapping.id" -> "mid")


    //新建MovieTypeName 表名
    val movieTypeName = s"$indexName/$ES_TAG_TYPE_NAME"

    //通过sparkSql 落地ES
    esMovieDF.write
      .options(movieOptions)
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(movieTypeName)




  }

}
