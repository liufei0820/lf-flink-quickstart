package com.lf.test.sideoutput

import com.lf.test.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Classname SideOutputTest
 * @Date 2020/8/27 下午4:12
 * @Created by fei.liu
 */
object SideOutputTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    // 用processFunction的侧输出流实现分流操作
    val highTempStream = dataStream
                            .process(new SplitTempProcessor(30))

    val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("low-temp"))

    // 打印输出
    highTempStream.print("high")
    lowTempStream.print("low")

    env.execute("side output stream")
  }
}

// 自定义ProcessFunction，用于区分高低温度的数据
class SplitTempProcessor(threshold: Int) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {

    // 判断当前数据的温度值，如果大于阈值，输出到主流，如果小于阈值，输出到侧输出流
    if (value.temp > threshold) {
      out.collect(value)
    } else {
      ctx.output(new OutputTag[(String, Double, Long)]("low-temp"), (value.id, value.temp, value.timestamp))
    }
  }
}



