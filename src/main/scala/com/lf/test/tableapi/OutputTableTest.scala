package com.lf.test.tableapi

import com.lf.test.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * @Classname OutputTabeTest
 * @Date 2020/9/1 下午1:47
 * @Created by fei.liu
 */
object OutputTableTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.readTextFile("/Users/apple/Documents/git-source/my-source/lf-flink-quickstart/src/main/resources/sensor.txt")

    val dataStream = inputStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    // 创建表环境
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 将DataStream转换成table
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'timestamp as 'ts, 'temp as 'temperature)

    val resultTable = sensorTable.select("id, temperature").filter("id = 'sensor_1'")

    val aggResultTable = sensorTable.groupBy("id").select('id, 'id.count as 'count)

    // 定义一张输出表，这就是要写入数据的TableSink
    tableEnv.connect(new FileSystem().path("/Users/apple/Documents/git-source/my-source/lf-flink-quickstart/src/main/resources/out.txt"))
        .withFormat(new Csv())
        .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE()))
        .createTemporaryTable("outputTable")

    // 将结果写入结果表
    resultTable.insertInto("outputTable")

//    sensorTable.printSchema()
//    sensorTable.toAppendStream[(String, Long, Double)].print()

    env.execute("output")
  }

}
