package com.lf.test.sink

import java.util

import com.lf.test.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * @Classname RedisSinkTest
 * @Date 2020/8/17 下午4:31
 * @Created by fei.liu
 */
object EsSinkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream = env.readTextFile("/path")
    val dataStream = inputStream.map(
      data => {
        val dataArr = data.split(",")
        SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
      }
    )

    // es
    // http host
    val hosts = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("localhost", 9200))

    // ElasticSearchSinkFunction
    val esSinkFunction = new ElasticsearchSinkFunction[SensorReading] {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        // 包装写入es的数据
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("sensor_id", element.id)
        dataSource.put("temp", element.temp.toString)
        dataSource.put("ts", element.timestamp.toString)

        // create indexRequest
        val indexRequest = Requests.indexRequest()
          .index("sensor_temp")
          .`type`("readingdata")
          .source(dataSource)
        indexer.add(indexRequest)
      }
    }

    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](hosts, esSinkFunction).build())

    env.execute("es test")
  }

}
