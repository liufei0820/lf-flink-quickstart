package com.lf.test.tableapi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}

/**
 * @Classname TableApiTest
 * @Date 2020/8/31 下午7:37
 * @Created by fei.liu
 */
object TableApiTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv = StreamTableEnvironment.create(env)

    // 创建表环境
    // 1、创建老版本的流查询环境
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 2、创建老版本的批式查询环境
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val batchEnvTable = BatchTableEnvironment.create(batchEnv)

    // 3、创建blink版本的流查询环境
    val bsSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)

    // 4、创建blink版本的批式查询环境
    val bbSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
//    val bbTableEnv = TableEnvironment.create(bbSettings)


  }

}
