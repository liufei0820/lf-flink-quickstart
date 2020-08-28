package com.lf.test.state

import com.lf.test.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
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

    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream = inputStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })


    env.execute("state test")
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



