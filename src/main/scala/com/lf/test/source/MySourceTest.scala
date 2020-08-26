package com.lf.test.source

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Classname MySourceTest 自定义source
 * @Date 2020/8/11 下午2:25
 * @Created by fei.liu
 */
object MySourceTest {

  def main(args: Array[String]): Unit = {

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val myStream = env.addSource(new MySensorSource())
//        .assignAscendingTimestamps(_.timestamp * 1000)
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
        })

    myStream.print("mystream")

    env.execute("my source test")

  }
}

class MySensorSource() extends SourceFunction[SensorReading] {

  var running : Boolean = true

  // 随机生成数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 随机数
    val random = new Random()

    // 随机生成10个传感器的温度值，并且不停在之前的温度基础上更逊（随机上下波动）
    // 首先生成10个传感器的初始温度
    var curTemps = 1.to(10).map(
      i => ("sensor_" + i, 60 + random.nextGaussian() * 20)
    )

    // 生成数据
    while (running) {
      // 在当前温度基础上随机生成微小波动
      curTemps = curTemps.map(
        data => (data._1, data._2 + random.nextGaussian())
      )

      // 获取当前时间
      val curTs = System.currentTimeMillis()

      // 包装成样例类
      curTemps.foreach(
        data => ctx.collect(SensorReading(data._1, curTs, data._2))
      )

      // 间隔时间
      Thread.sleep(1000L)
    }
  }

  override def cancel(): Unit = running = false
}
