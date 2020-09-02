package com.lf.test.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @Classname KafkaTableTest
 * @Date 2020/9/1 下午3:39
 * @Created by fei.liu
 */
object KafkaTableTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 创建表环境
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

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
      .createTemporaryTable("kafkaInputTable")

    // 转换操作
    // 将DataStream转换成table
    val sensorTable = tableEnv.from("kafkaInputTable")

    val resultTable = sensorTable.select("id, temperature").filter("id = 'sensor_1'")
    val aggResultTable = sensorTable.groupBy("id").select('id, 'id.count as 'count)

    // 定义结果表
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sinkTest")
      .property("bootstrap.servers", "localhost:9092")
      .property("zookeeper.connect", "localhost:2181"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaOutputTable")

    // 将结果表输出
    resultTable.insertInto("kafkaOutputTable")

    env.execute("kafka table test")
  }

}
