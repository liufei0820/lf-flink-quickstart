package com.lf.test.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, OldCsv, Schema}

/**
 * @Classname TableApiTest2
 * @Date 2020/8/31 下午7:56
 * @Created by fei.liu
 *
 * 从外部系统中读取数据，在环境中注册表
 */
object TableApiTest2 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    // 1、连接到文件系统（csv）
    val filePath = "/Users/apple/Documents/git-source/my-source/lf-flink-quickstart/src/main/resources/sensor.txt"

//    tableEnv.connect(new FileSystem().path(filePath))
//      .withFormat(new OldCsv) // 定义读取数据之后的格式方法
//      .withSchema(new Schema().field("id", DataTypes.STRING())
//        .field("timestamp", DataTypes.BIGINT())
//        .field("temperature", DataTypes.DOUBLE())) // 定义表结构
//      .createTemporaryTable("inputTable")              // 注册一张表

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())) // 定义表结构
      .createTemporaryTable("inputTable")

    // 装换成打印输出
    val table = tableEnv.from("inputTable")

    table.toAppendStream[(String, Long, Double)].print()

    env.execute("table test")

  }

}
