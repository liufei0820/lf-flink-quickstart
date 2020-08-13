package com.lf.test.transfor

import com.lf.test.source.SensorReading
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

/**
 * @Classname TransformTest
 * @Date 2020/8/11 下午3:52
 * @Created by fei.liu
 */
object TransformTest {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val source = environment.readTextFile("/path")

    val dataStream = source.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(1).toDouble)
    })

    // 分流
    val splitStream = dataStream.split(data => {
      if (data.temp > 30) {
        Seq("high")
      } else {
        Seq("low")
      }
    })

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")

    val allTempStream = splitStream.select("high", "low")

    highTempStream.print("high")
    lowTempStream.print("low")
    allTempStream.print("all")


    // 合流
    val warningStream : DataStream[(String, Double)] = highTempStream.map(
      data => (data.id, data.temp)
    )

    val connectedStreams = warningStream.connect(lowTempStream)
    val resultStream = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "high temp warning"),
      lowData => (lowData.id, "normal")
    )

    environment.execute("temp")

  }

}

class MyIDSelector() extends KeySelector[SensorReading, String] {
  override def getKey(in: SensorReading): String = in.id
}



