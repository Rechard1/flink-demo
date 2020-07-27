package com.yaowb.flink.test

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TransformTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val streamData = env.readTextFile("D:\\java-work\\FlinkDemo\\src\\main\\resources\\sensor.txt")


    //map
    val dataStream = streamData.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0), dataArray(1).trim.toLong, dataArray(2).trim.toDouble) //因为会有空格，先使用trim方法去掉空格
    })

    //聚合操作
    val aggrStream = dataStream.keyBy("id")
      //      .sum("temperature")
      .reduce((x, y) => {
        SensorReading(x.id, x.timeStamp + 1, y.temperature + 10)
      })


    //分流 split和 select   按照温度30来划分
    val splitStream = dataStream.split(sensorData => {
      if (sensorData.temperature > 30) {
        Seq("high")
      } else {
        Seq("low")
      }
    })

    val highStream = splitStream.select("high")
    val lowStream = splitStream.select("low")
    val allStream = splitStream.select("high", "low")


    //    aggrStream.print("high: " + highStream)
    //    aggrStream.print("low: " + lowStream)
    //    aggrStream.print("all: " + allStream)


    //合并分流 connect和 CoMap   将两条流合并成一条 允许两条流的数据类型不同
    val warningStream = highStream.map(sendorData => (sendorData.id, sendorData.temperature))
    val connectedData = warningStream.connect(lowStream)

    val connectedStreams = connectedData.map(
      warningData => (warningData._1, warningData._2, "high temperature warning"),
      lowData => (lowData.id, "healthy")
    )

    //union 合并多条流，每条流的数据类型必须相同
    val unionStream= highStream.union(lowStream)
    unionStream.print()





//    connectedStreams.print()

    env.execute("transform test")

  }
}
