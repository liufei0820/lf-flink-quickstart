package com.lf.test.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

/**
 * @Classname TableScanTest
 * @Date 2020/9/1 上午8:59
 * @Created by fei.liu
 */
object TableScanTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    val filePath = "/Users/apple/Documents/git-source/my-source/lf-flink-quickstart/src/main/resources/sensor.txt"


    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())) // 定义表结构
      .createTemporaryTable("inputTable")

    // 表的查询
    val sensorTable = tableEnv.from("inputTable")
    sensorTable.select('id, 'temperature)
      .filter('id === "")


    // sql简单查询下
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin)

    resultSqlTable.toAppendStream[(String, Double)]

    // 简单聚合
    val aggResultTable = sensorTable.groupBy("id")
      .select('id, 'id.count as 'count)

    // sql实现简单聚合
    val aggResultSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt from inputTable group by id")
    aggResultSqlTable.toRetractStream[(String, Long)].print("agg")

    env.execute("table-scan")

  }
}
