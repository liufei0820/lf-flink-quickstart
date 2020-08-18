package com.lf.test.sink

import com.lf.test.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @Classname RedisSinkTest
 * @Date 2020/8/17 下午4:31
 * @Created by fei.liu
 */
object RedisSinkTest {

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

    // redis 配置
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379).build()

    // redis mapper
    val myMapper = new RedisMapper[SensorReading] {
      // 定义保存数据到redis的命令 hset table_name key value
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
      }

      override def getKeyFromData(data: SensorReading): String = data.id

      override def getValueFromData(data: SensorReading): String = data.temp.toString
    }

    dataStream.addSink(new RedisSink[SensorReading](config, myMapper))

    env.execute("redis test")
  }

}
