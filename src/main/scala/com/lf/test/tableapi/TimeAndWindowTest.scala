package com.lf.test.tableapi

import com.lf.test.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @Classname TimeAndWindowTest
 * @Date 2020/9/1 下午4:55
 * @Created by fei.liu
 */
object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("/Users/apple/Documents/git-source/my-source/lf-flink-quickstart/src/main/resources/sensor.txt")

    val dataStream = inputStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    })

    // 创建表环境
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 将DataStream转换成Table
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'timestamp as 'ts, 'temp, 'et.rowtime)
//    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temp)

    sensorTable.printSchema()
    sensorTable.toAppendStream[Row].print("row")
    env.execute("time and window test")

  }
}
