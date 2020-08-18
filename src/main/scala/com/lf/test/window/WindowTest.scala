package com.lf.test.window

import com.lf.test.source.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * @Classname WindowTest
 * @Date 2020/8/17 下午7:52
 * @Created by fei.liu
 */
object WindowTest {

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

    val resultStream = dataStream.keyBy("id")
      .window()
  }

}
