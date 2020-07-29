package com.yaowb.flink.window

import org.apache.flink.streaming.api.scala._
import com.yaowb.flink.test.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //插入watermark的周期为200毫秒
    env.getConfig.setAutoWatermarkInterval(200L)


    val inputStream = env.readTextFile("D:\\java-work\\FlinkDemo\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      //        override def extractTimestamp(t: SensorReading): Long = t.timeStamp*1000L
      //      })
      .assignTimestampsAndWatermarks(new myAssigner())
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
//      .timeWindow(Time.seconds(10))    //滚动窗口  统计10秒内最小的温度
      .timeWindow(Time.seconds(15),Time.seconds(5))  //滑动窗口  统计15秒内最小的温度，隔5秒输出一次
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    dataStream.print()

    env.execute("window test")
  }
}

/**
 * 周期性生成Watermark
 */
class myAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  //定义固定延时为3秒
  val bound = 30000

  //定义当前收到的最大时间戳
  var maxTimeStamps = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTimeStamps - bound)

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTimeStamps = maxTimeStamps.max(t.timeStamp * 1000L)
    t.timeStamp * 1000L
  }
}

/**
 * 来一条就生成一个Watermark，对每条进行筛选
 */
class myAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  val bound: Long = 1000L

  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
    if (t.id == "sensor_1") {
      new Watermark(l - bound)
    } else {
      null
    }
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timeStamp * 1000L
  }
}