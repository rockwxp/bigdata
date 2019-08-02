package com.rock.streaming

import com.mongodb.casbah.commons.MongoDBObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
//java和scala直接的方法相互转换
import scala.collection.JavaConversions._

/**
  * description: 实时推荐引擎
  * author: Rock
  * create: 2019-07-31 20:48
  **/
object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  //用户最近评分的数量
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"


  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[3]",
      "kafka.topic" -> "recom",
      "mongo.uri" -> "mongodb://bigdata113:27017/recom",
      "mongo.db" -> "recom"
    )

    val kafkaPara = Map(
      "bootstrap.servers" -> "bigdata113:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recomgroup"
    )

    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster("local[3]").set("spark.executor.memory", "4g")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._
    implicit val mongConfig = MongConfig(config("mongo.uri"), config("mongo.db"))
    //制作共享变量 获取所有离线对电影相似度集合
    val simMoviesMatrix = spark.read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs].rdd
      .map { recs =>
        (recs.mid, recs.recs.map(x => (x.mid, x.r)).toMap)
      }.collectAsMap()
    //广播变量 让所有的服务节点有相同的数据 保存所有电影即和它相似的电影列表
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

    //用count触发广播
    val abc = sc.makeRDD(1 to 2)
    abc.map(x => simMoviesMatrixBroadCast.value.get(1)).count()

    //Kafka链接
    val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))

    //接收Kafka的评分流 UID|MID|SCORE|TIMESTAMP
    var ratingStream = kafkaStream.map {
      case mage => {
        val attr = mage.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    }

    //测试：Kafka消费 ./bin/kafka-console-producer.sh --broker-list bigdata113:9092 --topic recom
    ratingStream.foreachRDD {
      rdd =>
        rdd.map {
          //获取当前用户评分数据
          case (uid, mid, score, timestamp) =>
            println(uid)


            //开始计算
            //获取用户最近的n次的评分电影的集合（实时）
            val userRecentlyRatings = getUserResently(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

            //在离线的电影相似度集合中 获取n个与当前用户所评价的电影的最相似并且没有被该用户评价过的电影ID集合
            var simMovies = getOffSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadCast.value)

            //计算推荐电影的优先级
            computMovieScore(simMoviesMatrixBroadCast.value,simMovies,userRecentlyRatings)




          //将推荐电影数据保存MongoDB


        }.count()
    }


    ssc.start()
    ssc.awaitTermination()


  }


  /**
    * @Description: 获取用户没有评价过的电影ID集合
    * @param: [num, mid, uid, simMoviesMatrix, mongoConfig]
    * @return: scala.Function1<com.rock.streaming.MongConfig,int[]>
    * @auther: Rock
    * @date: 2019-08-02 08:05
    */
  def getOffSimMovies(num: Int, mid: Int, uid: Int, simMoviesMatrix: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongConfig) = {
    //从广播的电影相似度矩阵中获取用户评价的电影的电影相似矩阵
    val allSimMovies = simMoviesMatrix.get(mid).get.toArray

    //获取用户已经评过分的电影ID
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid)).toArray.map {
      item =>
        item.get("mid").toString.toInt
    }

    //过滤掉用户已经评价过掉电影，进行相似度排序，取前num给电影，返回电影ID
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)

  }


  /**
    * @Description: 从redis中获取用户最近的电影评分集合
    * @param: [num 获取数据的数量, uid 用户ID, jedis redis链接器]
    * @return:
    * @auther: Rock
    * @date: 2019-08-01 14:14
    */
  def getUserResently(num: Int, uid: Int, jedis: Jedis) = {

    //redis中保存的数据格式：uid:1  100:5.0,200:4.9.....
    jedis.lrange("uid:" + uid.toString, 0, num).map {
      item =>
        var attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)

    } toArray

  }

  /**
    * @Description: 在相似度矩阵中查出两个电影的相似度
    * @param: [simMovies 相似度矩阵, mid, resentMid]
    * @return: double
    * @auther: Rock
    * @date: 2019-08-01 17:43
    */
  def getMoviesSimScore(simMoviesMatrix: collection.Map[Int, Map[Int, Double]], mid: Int, resentMid: Int) = {

    simMoviesMatrix.get(mid) match {
      case Some(simGroup) => simGroup.get(resentMid) match {
        case Some(sim) => sim
        case None => 0.0
      }

      case None => 0.0
    }

  }

  def computMovieScore(simMoviesMatrix: collection.Map[Int, Map[Int, Double]], recommendMovieIDs: Array[Int], recentlyMovieRatings: Array[(Int, Double)]) = {

    //记录实时推荐电影的ID和分数
    val score = ArrayBuffer[(Int, Double)]()

    //记录电影ID的增强因子
    val incrMap = mutable.HashMap[Int, Int]()

    //记录电影ID的减弱因子
    val decrMap = mutable.HashMap[Int, Int]()

    for (mid <- recommendMovieIDs; recentMovie <- recentlyMovieRatings) {

      //获取推荐电影和用户最近评分电影的相似度（百分比）
      val simRate = getMoviesSimScore(simMoviesMatrix, mid, recentMovie._1)

      if (simRate > 0.6) {
        //计算电影分数
        val mScore = recentMovie._2 * simRate
        //保存推荐电影集合
        score += ((mid, mScore))

        //判断增强或减弱
        if (recentMovie._2 > 3) {
          //在增强map中增加电影id和增加值，增加之前先判断是否有值，如果没，先赋默认值0
          incrMap(mid) = incrMap.getOrDefault(mid,0)+1
        }else{
          //同理，
          decrMap(mid) = decrMap.getOrDefault(mid,0)+1
        }
      }
    }

    score.groupBy(_._1).map{
      case (mid,scores) =>
        (mid,score.map(_._2).sum/score.size + log(incrMap(mid)) -log(decrMap(mid)))
    }.toArray

  }

  def log(m:Int) : Double ={
    math.log(m) / math.log(2)
  }


}
