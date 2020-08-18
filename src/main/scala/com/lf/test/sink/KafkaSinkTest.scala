package com.lf.test.sink

import com.lf.test.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * @Classname KafkaSinkTest
 * @Date 2020/8/17 下午1:27
 * @Created by fei.liu
 */
object KafkaSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream = env.readTextFile("/path")
    val dataStream = inputStream.map(
      data => {
        val dataArr = data.split(",")
        SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble).toString
      }
    )

    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "kafkasink test", new SimpleStringSchema()))

    env.execute("sink test")
  }

}
