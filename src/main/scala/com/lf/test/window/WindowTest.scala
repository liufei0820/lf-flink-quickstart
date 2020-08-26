package com.lf.test.window

import java.util.Random

import com.lf.test.source.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Classname WindowTest
 * @Date 2020/8/17 下午7:52
 * @Created by fei.liu
 */
object WindowTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500l)

    val inputStream = env.addSource(new MySensorSource())

    val resultStream = inputStream.keyBy("id")
//      .window(EventTimeSessionWindows.withGap(Time.minutes(1))) // 会话创建
      .timeWindow(Time.seconds(15))
//      .reduce(new MyReduce())
        .apply(new MyWindowFunction)

    resultStream.print()

    env.execute("window test")
  }

}

class MyReduce() extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value1.timestamp.max(value2.timestamp), value1.temp.min(value2.temp))

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

// 自定义一个全窗口函数
class MyWindowFunction extends WindowFunction[SensorReading, (Long, Int), Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long, Int)]): Unit = {

    out.collect((window.getStart, input.size))
  }
}

// 自定义一个周期性生成watermark的Assigner
class MyWMAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  // 需要两个关键参数，延迟时间，和当前所有数据中最大的时间戳
  val lateness: Long = 1000
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - lateness)

  override def extractTimestamp(element: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000)
    element.timestamp * 1000
  }
}

