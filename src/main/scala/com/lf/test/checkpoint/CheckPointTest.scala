package com.lf.test.checkpoint

import java.util.concurrent.TimeUnit

import com.lf.test.processfunction.TempIncreWarning
import com.lf.test.source.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

/**
 * @Classname CheckPointTest
 * @Date 2020/8/30 上午8:49
 * @Created by fei.liu
 */
object CheckPointTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // checkpoint相关配置
    // 启用checkpoint检查点，指定检查点间隔时间(毫秒)
    env.enableCheckpointing(1000)
    // 其他配置
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(30000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    // checkpoint 重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))


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
