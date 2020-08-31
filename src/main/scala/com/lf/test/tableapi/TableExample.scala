package com.lf.test.tableapi

import com.lf.test.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
 * @Classname TableExample
 * @Date 2020/8/31 上午9:13
 * @Created by fei.liu
 */
object TableExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val inputStream = env.readTextFile("/Users/apple/Documents/git-source/my-source/lf-flink-quickstart/src/main/resources/sensor.txt")
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    // 创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    // 基于数据流，转换成一张表，然后进行操作
    val dataTable = tableEnv.fromDataStream(dataStream)
    // 调用table API，得到转换结果
    val resultTable = dataTable.select("id, temp")
      .filter("id == 'sensor_1'")

    // 或者直接写sql得到转换结果
    val resultSqlStream = tableEnv.sqlQuery("select id, temp from " + dataTable + " where id = 'sensor_1'")

    resultSqlStream.printSchema()
    val result = resultSqlStream.toAppendStream[(String, Double)]
    result.print("result")

    resultTable.printSchema()

    val resultStream = resultTable.toAppendStream[(String, Double)]

    resultStream.print("resultStream")

    env.execute("table test")

  }

}
