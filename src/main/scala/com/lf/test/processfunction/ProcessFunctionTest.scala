package com.lf.test.processfunction

import com.lf.test.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Classname ProcessFunctionTest
 * @Date 2020/8/26 下午4:21
 * @Created by fei.liu
 */
object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream = inputStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    // 检测每个传感器温度值是否连续上升，在10s内
    val warningStream = dataStream.keyBy("id")
      .process(new TempIncreWarning(10000L))

    warningStream.print()
    env.execute("process function job")

  }

}

// 自定义 keyedProcessFunction
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {

  // 由于需要跟之前的温度值做对比，所以将上一次温度保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  // 为了方便删除定时器，还需要保存定时器的时间戳
  lazy val curTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-time-ts", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    // 首先取出状态
    val lastTemp = lastTempState.value()
    val curTimerTs = curTimerTsState.value()

    // 将上次温度值的状态更新未当前数据的温度值
    lastTempState.update(value.temp)

    // 判断当前温度值，如果比之前温度高，并且没有定时器的话，注册10s后的定时器
    if (value.temp > lastTemp && curTimerTs == 0) {
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)

      curTimerTsState.update(ts)
    }
    // 如果温度下降，删除定时器
    else if (value.temp < lastTemp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      // 清空状态
      curTimerTsState.clear()
    }
  }

  // 定时器触发，说明10s内没有来下降的温度值，报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("温度值连续" + interval / 1000 + "秒上升")
    curTimerTsState.clear()
  }
}



