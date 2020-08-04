package com.yaowb.flink.test

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector


/**
 *测输出流--当温度降到32度以下，通过测输出流输出报警信息
 */
object SideOutPutStreamTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timeStamp * 1000
      })

    val processStream = dataStream.process(new FreezingAlert)

    processStream.print("processed data")
    processStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")


    env.execute("side out put stream test")

  }
}

//温度低于32度，输出报警信息到测输出流
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading] {
  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temperature < 32.0) {
      //测输出流的输出，需要使用上下文的output方法，并且使用OutputTag类的实例将所要输出的信息封装
      context.output(new OutputTag[String]("freezing alert"), "freezing alert for " + i.id)
    }
  }
}