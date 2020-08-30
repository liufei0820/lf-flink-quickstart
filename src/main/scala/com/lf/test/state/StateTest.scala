package com.lf.test.state

import com.lf.test.source.SensorReading
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Classname StateTest
 * @Date 2020/8/28 上午9:10
 * @Created by fei.liu
 */
object StateTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    /**
     * set state backend
     */
//    env.setStateBackend(new MemoryStateBackend())
//    env.setStateBackend(new FsStateBackend("path"))
//    env.setStateBackend(new RocksDBStateBackend())


    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream = inputStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    val warningStream = dataStream.keyBy("id")
//      .map(new TempChangeWarning(10.0))
//        .flatMap(new TempChangeWarningWithFlatMap(10.0))
        .flatMapWithState[(String, Double, Double), Double]({
          case (inputData: SensorReading, None) => (List.empty, Some(inputData.temp))
          case (inputData, lastTemp: Some[Double]) => {
            val diff = (inputData.temp - lastTemp.get).abs
            if (diff > 10.0) {
              (List((inputData.id, lastTemp.get, inputData.temp)), Some(inputData.temp))
            } else {
              (List.empty, Some(inputData.temp))
            }
          }
        })

    warningStream.print("warning")

    env.execute("state test")
  }

}

class TempChangeWarning(threshold: Double) extends RichMapFunction[SensorReading, (String, Double, Double)] {

  // 定义状态变量，上一次的温度值
  private var lastTempState: ValueState[Double] = _


  override def open(parameters: Configuration): Unit = {

    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  }

  override def map(value: SensorReading): (String, Double, Double) = {

    // 从状态中获取上次的温度值
    val lastTemp = lastTempState.value()
    // 更新状态
    lastTempState.update(value.temp)

    // 与当前温度值计算差值，然后与阈值比较，如果超过，就报警
    val diff = (value.temp - lastTemp).abs
    if (diff > threshold) {
      (value.id, lastTemp, value.temp)
    } else {
      (value.id, 0.0, 0.0)
    }
  }
}

// 自定义RichFlatMapFunction，可以输出多个结果
class TempChangeWarningWithFlatMap(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    // 更新状态
    lastTempState.update(value.temp)

    val diff = (value.temp - lastTemp).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temp))
    }
  }
}



class MyProcessor extends KeyedProcessFunction[String, SensorReading, Int] {

//  lazy val myState: ValueState[Int] = getRuntimeContext
  var myState: ValueState[Int] = _


  override def open(parameters: Configuration): Unit = {

    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))
  }

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, Int]#Context, collector: Collector[Int]): Unit = {
    myState.value()
    myState.clear()
  }
}



