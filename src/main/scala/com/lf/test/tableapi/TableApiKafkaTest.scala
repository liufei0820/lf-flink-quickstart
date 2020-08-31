package com.lf.test.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @Classname TableApiKafkaTest
 * @Date 2020/8/31 下午8:31
 * @Created by fei.liu
 */
object TableApiKafkaTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("bootstrap.servers", "localhost:9092")
      .property("zookeeper.connect", "localhost:2181"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    val table = tableEnv.from("inputTable")

    table.toAppendStream[(String, Long, Double)].print("result")

    env.execute("table-api-kafak-test")

  }

}
