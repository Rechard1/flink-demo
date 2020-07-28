package com.yaowb.flink.sinkdemo

import java.util

import com.yaowb.flink.test.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.{Request, Requests}

object ESSinkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //Source from file
    val inputStream = env.readTextFile("D:\\java-work\\FlinkDemo\\src\\main\\resources\\sensor.txt")

    //TransForm
    val dataSTream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(1).trim.toDouble)
    })

    //配置http连接
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))


    //配置ES中的数据结构
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

          print("Save data: " + t)
          val json = new util.HashMap[String, String]()
          json.put("sensor_id", t.id)
          json.put("temperature", t.temperature.toString)
          json.put("time_stamp", t.timeStamp.toString)

          val indexRequest = Requests.indexRequest().index("sensor").`type`("readingdata").source(json)

          requestIndexer.add(indexRequest)
          print("data save")

        }
      }
    )

    //sink into elastic search
    dataSTream.addSink(esSinkBuilder.build())

    env.execute("sink into es test")
  }
}
