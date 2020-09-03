package com.lf.test.tablewindow

import java.sql.Timestamp

import com.lf.test.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @Classname WindowTest
 * @Date 2020/9/2 下午2:04
 * @Created by fei.liu
 */
object WindowTest {
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

    // 窗口操作
    // 1、group 窗口, 开一个滚动窗口，10s的滚动窗口，统计每个传感器温度的数量
    val groupResultTable = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'tw.end)

    // 打印输出
//    groupResultTable.toRetractStream[(String, Long, Timestamp)].print("group-result")

    // 2、Group 窗口 SQL实现
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |id,
        |count(id),
        |tumble_end(ts, interval '10' second)
        |from sensor
        |group by
        |id,
        |tumble(ts, interval '10' second)
        |""".stripMargin)
//    resultSqlTable.toAppendStream[Row].print("sql-result")

    // 3、over窗口，对每个传感器统计每一行数据与前两行数据的平均温度
    val resultOver = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w)
      .select('id, 'ts, 'id.count over 'w, 'temp.avg over 'w)

    resultOver.toAppendStream[Row].print("result-over")


    // 4、over窗口 sql实现
    val sqlResultOver = tableEnv.sqlQuery(
      """
        |select id, ts,
        |count(id) over w,
        |avg(temp) over w
        |from sensor
        |window w as (
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        |)
        |""".stripMargin)

    sqlResultOver.toAppendStream[Row].print("sql-result-over")

    env.execute("window test")

  }
}
