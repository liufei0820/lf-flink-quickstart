package com.lf.test.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.lf.test.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink

/**
 * @Classname RedisSinkTest
 * @Date 2020/8/17 下午4:31
 * @Created by fei.liu
 */
object JdbcSinkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream = env.readTextFile("/path")
    val dataStream = inputStream.map(
      data => {
        val dataArr = data.split(",")
        SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
      }
    )


    dataStream.addSink(new MyJdbcSink)

    env.execute("jdbc test")
  }

}

class MyJdbcSink() extends RichSinkFunction[SensorReading] {

  // 首先定义sql连接，以及预编译语句
  var conn : Connection = _
  var insertStmt : PreparedStatement = _
  var updateStmt : PreparedStatement = _

  // 在open生命周期内创建连接以及预编译语句
  override def open(parameters: Configuration): Unit = {

    conn = DriverManager.getConnection("jdbc://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into temp (sensor, temperature) values (?, ?)")
    updateStmt = conn.prepareStatement("update temp set temperature = ? where sensor = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 执行更新语句
    updateStmt.setDouble(1, value.temp)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    // 如果刚才没有更新
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temp)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}



