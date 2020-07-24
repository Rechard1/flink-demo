package com.yaowb.flink.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

case class SensorReading(id: String, timeStamp: Long, temperature: Double)

object Sendsor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val stream1 = env.fromCollection(List(
//      SensorReading("sensor_1", 1547718199, 35.80018327300259),
//      SensorReading("sensor_6", 1547718201, 15.402984393403084),
//      SensorReading("sensor_7", 1547718202, 6.720945201171228),
//      SensorReading("sensor_10", 1547718205, 38.101067604893444)
//    ))
//
//    //    env.fromElements("dsa",'d',1,32,0.100) //从元素中读取，不分任何数据类型。
//
//    stream1.print("stream1").setParallelism(5) //打印，设置并行
//



    //Kafka Source
//    readFromKafka



    //自定义Source
    val myStreamData= env.addSource(new MySensorSource)

    myStreamData.print().setParallelism(3)

    env.execute()

  }


  def readFromKafka: Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092,slave3:9092")
    properties.setProperty("group.id", "WriteToES")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val envKafka = StreamExecutionEnvironment.getExecutionEnvironment

    val topic:String ="flow_tianrongxin"

    val kafkaStreamData= envKafka.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties))

    kafkaStreamData.print().setParallelism(2)

    envKafka.execute("kafka source test")


  }





}

/**
 * 自定义源数据
 */
class MySensorSource extends SourceFunction[SensorReading]{

  //表示数据源是否还在运行
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand: Random = new Random()

//  当前的温度，一次产生10条
    var currentTemperature= 1.to(10).map(
      i => ("sensor" + i, 65 + rand.nextGaussian() * 20)
    )

    while(running) {
      //更新温度
      currentTemperature=currentTemperature.map(
        t=>(t._1,t._2+rand.nextGaussian())
      )

      val currentTime=System.currentTimeMillis()

      currentTemperature.foreach(
        t=>sourceContext.collect(SensorReading(t._1,currentTime,t._2))
      )

      Thread.sleep(100)
    }


  }

  override def cancel(): Unit = {
    running=false
  }
}
