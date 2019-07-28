package com.rock.test

import java.net.InetAddress

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


/**
  * description:
  * author: Rock
  * create: 2019-07-26 09:22
  **/
object ESTest {
  def main(args: Array[String]): Unit = {
    //创建全局配置
    val params = scala.collection.mutable.Map[String, Any]()
    params += "spark.cores" -> "local[2]"
    params += "mongo.uri" -> "mongodb://bigdata113:27017/recom"
    params += "mongo.db" -> "recom"
    params += "es.httpHosts" -> "bigdata113:9200"
    params += "es.transportHosts" -> "bigdata113:9300"
    params += "es.index" -> "recom" //ES库名
    params += "es.cluster.name" -> "my-application" //ES配置文件elasticsearch.yml cluster.name: my-application


    val settings = Settings.builder().put("cluster.name", params("es.cluster.name").toString).build()
    val esClient = new PreBuiltTransportClient(settings)
    val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r

    //获取ES集群内部每台通讯地址
    params("es.transportHosts").toString.split(",").foreach {
      //通过正则获取参数，在定义正则中的每个括号，对应下面的每个参数
      case ES_HOST_PORT_REGEX(host: String, port: String) =>
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
    }

    //判断Index表名是否存在，如果存在就删除表
    if(esClient.admin().indices().exists(new IndicesExistsRequest("recom")).actionGet()isExists){
      //删除Index
      esClient.admin().indices().delete(new DeleteIndexRequest("recom")).actionGet()
    }


  }

}
