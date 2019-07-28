package com.rock.statistics

import java.text.SimpleDateFormat
import java.util.Date

import com.rock.statistics.StatisticsApp.spark
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * description:
  * author: Rock
  * create: 2019-07-27 11:02
  **/
object staticsRecommender {

  val RATE_MORE_MOVIES="RateMoreMovies"
  val RATE_MORE_MOVIES_RECENTLY="RateMoreMoviesRecently"
  val AVERAGE_MOVIES_SCORE = "AverageMoviesScore"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def rateMore(spark:SparkSession)(implicit mongoConfig:MongoConfig): Unit ={
    //以MovieID分组，用评分次数排序，从大到小排序，
    val rateMoreDF= spark.sql("select mid,count(1) as count from ratings group by mid order by count desc")

    rateMoreDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }

  /**
   * @Description: 按照月统计，这个月中评分最多的电影我们认为是热门电影，统计每个月中的每个电影的评分数量   ->   RateMoreRecentlyMovie
   * @param: [Spark, mongoConfig]
   * @return: scala.Function1<com.rock.statistics.MongoConfig,scala.runtime.BoxedUnit>
   * @auther: Rock
   * @date: 2019-07-27 13:31
   */
  def rateMoreRecently(Spark:SparkSession)(implicit mongoConfig:MongoConfig): Unit ={

    val sdf = new SimpleDateFormat("yyyyMM")
    //定义时间转换方法changeDate
    spark.udf.register("changeDate",(x:Long)=>sdf.format(new Date(x*1000L)).toLong)

    //将用户对电影的评分的数据集中的时间戳改成yyyyMM
    val yearMonthOfRatings = spark.sql("select mid,uid,score,changeDate(timestamp) as yearmonth from ratings")
    //创建视图
    yearMonthOfRatings.createOrReplaceTempView("ymrating")
    //1.先以日期分组，然后电影ID分组，2.对日期排序，对用评分次数排序
    spark.sql("select mid,count(1) as count,yearmonth from ymrating group by yearmonth,mid order by yearmonth desc,count desc")
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_MOVIES_RECENTLY)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  /**
   * @Description: 统计每个电影类型top10的电影
   * @param: [spark, movies, mongoConfig]
   * @return: scala.Function1<org.apache.spark.sql.Dataset<com.rock.statistics.Movie>,scala.Function1<com.rock.statistics.MongoConfig,scala.runtime.BoxedUnit>>
   * @auther: Rock
   * @date: 2019-07-27 22:12
   */
  def genresTop10(spark: SparkSession)(movies:Dataset[Movie])(implicit mongoConfig: MongoConfig): Unit ={




    //查询出每个电影的平均评分，生成两个字段 电影ID 和 平均分数
    val averageMovieScoreDF = spark.sql("select mid,avg(score) as avg from ratings group by mid").cache()

    //与电影表合并，生成三个字段 电影ID 和 平均分数 和 电影类型
    val moviesWithScoreDF = movies.join(averageMovieScoreDF,Seq("mid","mid")).select("mid","avg","genres").cache()

    //定义所有电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    //将电影类别转换成RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)

    import spark.implicits._
    //根据电影类别RDD开始分类
    //笛卡尔积操作cartesian，将每个电影类别和moviesWithScoreDF每条 相乘
    val genresTopMovies =genresRDD.cartesian(moviesWithScoreDF.rdd).filter{
      case (genres,row) => {
        //filter 拦截过滤数据，将row(moviesWithScoreDF)中的电影类别中没和genres(genresRDD)的数据去除
        row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
    }.map{
      case (genres,row) =>{
        //只保留row(moviesWithScoreDF)中的电影ID 和 平均分数
        (genres,(row.getAs[Int]("mid"),row.getAs[Double]("avg")))
      }
    }.groupByKey()//分组，根据上一层处理返回的数据中第一个值genres 进行分组，对应每一个电影类型是电影的数据集合（电影ID 和 平均分数）
      .map{
        case (genres,items)=>{
          //封装数据到对象中，并且把items中电影数据集合转成List，根据评分保留前十到数据，然后倒叙排列
          GenresRecommendation(genres,items.toList.sortWith(_._2 > _._2).take(10).map(x => Recommendation(x._1,x._2)))
        }
      }toDF()

    genresTopMovies
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    averageMovieScoreDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", AVERAGE_MOVIES_SCORE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }

}
