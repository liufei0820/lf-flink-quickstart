package com.lf.test.wc

import org.apache.flink.api.scala._

/**
 * @Classname WordCount
 * @Date 2020/7/28 下午8:48
 * @Created by fei.liu
 */
// 批处理代码
object WordCount {

  def main(args: Array[String]): Unit = {

    // 创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val input = "/Users/apple/Documents/git-source/my-source/lf-flink-quickstart/src/main/resources/hello.txt"

    val inputDataSet = env.readTextFile(input)

    val wordCountDataSet = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    // 打印
    wordCountDataSet.print()
  }

}
