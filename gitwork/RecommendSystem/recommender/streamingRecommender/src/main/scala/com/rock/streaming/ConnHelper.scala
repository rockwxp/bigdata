package com.rock.streaming

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.Jedis

/**
  * description:
  * author: Rock
  * create: 2019-07-31 21:27
  **/
object ConnHelper extends Serializable {

    lazy val jedis = new Jedis("bigdata113")
    lazy val mongoClient = MongoClient(MongoClientURI("mongodb://bigdata113:27017/recom"))
}
