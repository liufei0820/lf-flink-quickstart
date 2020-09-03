package com.lf.test.function

import com.lf.test.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @Classname TableFunctionTest
 * @Date 2020/9/2 下午7:27
 * @Created by fei.liu
 */
object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val inputStream = env.readTextFile("/Users/apple/Documents/git-source/my-source/lf-flink-quickstart/src/main/resources/sensor.txt")

    val dataStream = inputStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })

    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temp)

    // 创建一个UDF的实例
    val split = new Split("_")
    // 调用Table API，TableFunction使用的时候需要用joinLateral方法
    val resultTable = sensorTable
      .joinLateral(split('id) as('word, 'length))
      .select('id, 'ts, 'word, 'length)

    // sql实例
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)
    val sqlResultTable = tableEnv.sqlQuery(
      """
        |select id, ts, word, length
        |from sensor, lateral table(split(id)) as splitid(word, length)
        |""".stripMargin)

    resultTable.toAppendStream[Row].print("table")
    sqlResultTable.toAppendStream[Row].print("sql")

    env.execute("table")
  }

}


// 实现一个自定义TableFunction，对一个String，输出用某个分隔符切分之后的（word, wordLength）
class Split(separator: String) extends TableFunction[(String, Int)] {
  def eval(str: String): Unit = {
    str.split(separator).foreach(
      word => collect((word, word.length))
    )
  }
}

