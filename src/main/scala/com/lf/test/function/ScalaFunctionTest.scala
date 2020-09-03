package com.lf.test.function

import com.lf.test.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * @Classname ScalaFunctionTest
 * @Date 2020/9/2 下午7:12
 * @Created by fei.liu
 */
object ScalaFunctionTest {

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
    val hashCode = new HashCode(10)
    // 调用Table API
    val resultTable = sensorTable.select('id, 'ts, hashCode('id))

    // sql实现
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashcode", hashCode)
    val resultSqlTable = tableEnv.sqlQuery("select id, ts, hashcode(id) from sensor")

    resultTable.toAppendStream[Row].print("result-table")
    resultSqlTable.toAppendStream[Row].print("result-sql-table")

    env.execute("function")
  }
}

// 自定义标亮函数
class HashCode(factor : Int) extends ScalarFunction {
  // 必需要实现一个eval方法，它的参数是当前传入的字段，它的输出是一个Int类型的hash值
  def eval(str: String): Int = {
    str.hashCode * factor
  }
}

