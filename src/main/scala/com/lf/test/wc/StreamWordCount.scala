package com.lf.test.wc

import org.apache.flink.streaming.api.scala._

/**
 * @Classname StreamWordCount
 * @Date 2020/7/28 下午9:03
 * @Created by fei.liu
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收socker数据流
    val textStream = env.socketTextStream("localhost", 7777)

    // 读取数据，wordcount
    val streamWordCountDataSet = textStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1))
      .keyBy(0).sum(1)

    streamWordCountDataSet.print()

    env.execute("stream word count job")
    

  }

}
