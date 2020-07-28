package com.yaowb.flink.sinkdemo
import org.apache.flink.streaming.api.scala._
import com.yaowb.flink.test.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisPoolConfig, FlinkJedisSentinelConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * 数据输出Sink 到 Redis
 */
object RedisSinkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream=env.readTextFile("D:\\java-work\\FlinkDemo\\src\\main\\resources\\sensor.txt")

    val streamData=inputStream.map(data=> {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(3).trim.toDouble)
    })

    //配置Redis的链接属性
    val conf= new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()

    //Sink into Redis
    streamData.addSink(new RedisSink(conf, new MyRedisMapper))

    streamData.print()

    env.execute("sink into redis test")
  }
}


/**
 * 保存数据进Redis的数据格式设置
 * 1、采用什么数据类型
 * 2、以什么为Key和Value
 */
class MyRedisMapper extends RedisMapper[SensorReading]{
  //定义保存数据数据导Redis命令
  override def getCommandDescription: RedisCommandDescription = {
    //把传感器的温度和id保存为hash表结构
    new RedisCommandDescription(RedisCommand.HSET,"temperature")
  }

  //定义保存到reids的Key
  override def getKeyFromData(t: SensorReading): String = t.temperature.toString

  //定义保存到redis的Value
  override def getValueFromData(t: SensorReading): String = t.id
}
