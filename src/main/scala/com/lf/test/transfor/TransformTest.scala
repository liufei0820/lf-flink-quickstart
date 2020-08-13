package com.lf.test.transfor

import com.lf.test.source.{MySensorSource, SensorReading}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

/**
 * @Classname 分流合流
 * @Date 2020/8/11 下午3:52
 * @Created by fei.liu
 */
object TransformTest {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val source = environment.addSource(new MySensorSource())

    // 分流
    val splitStream = source.split(data => {
      if (data.temp > 30) {
        Seq("high")
      } else {
        Seq("low")
      }
    })

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")

    val allTempStream = splitStream.select("high", "low")

//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")


    // 合流
    val warningStream : DataStream[(String, Double)] = highTempStream.map(
      data => (data.id, data.temp)
    )

    val connectedStreams = warningStream.connect(lowTempStream)
    val resultStream = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "high temp warning"),
      lowData => (lowData.id, "normal")
    )

    resultStream.print("result")

    environment.execute("transform")

  }

}

class MyIDSelector() extends KeySelector[SensorReading, String] {
  override def getKey(in: SensorReading): String = in.id
}



