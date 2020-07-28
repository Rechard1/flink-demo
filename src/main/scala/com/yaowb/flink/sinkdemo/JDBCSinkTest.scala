package com.yaowb.flink.sinkdemo

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.streaming.api.scala._
import com.yaowb.flink.test.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 将数据Sink存进mysql数据库
 */
class JDBCSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream = env.readTextFile("D:\\java-work\\FlinkDemo\\src\\main\\resources\\sensor.txt")

    val streamData = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(3).trim.toDouble)
    })

    streamData.addSink(new MyJdbcSink)
    streamData.print()

    env.execute("sink into jdbc test")

  }
}

class MyJdbcSink extends RichSinkFunction[SensorReading] {
  //定义sql连接和预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //配置驱动以及需要执行额sql语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test\", \"root\", \"123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?,?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }


  //执行sql，有则更新，无则添加
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  //关闭连接
  override def clone(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
