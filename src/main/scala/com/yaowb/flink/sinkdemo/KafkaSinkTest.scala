package com.yaowb.flink.sinkdemo

import java.util.Properties

import com.yaowb.flink.test.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092,slave3:9092")
    properties.setProperty("group.id", "WriteToES")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val topic: String = "flow_tianrongxin"
    //Source from Kafka


    val inputStream = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties))

    //Transform
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // Sink into Kafka
//    dataStream.addSink(new FlinkKafkaProducer011[String]("kafka_sink_test", new SimpleStringSchema(), properties))

    dataStream.print()

    env.execute("sink into kafka test")
  }
}
