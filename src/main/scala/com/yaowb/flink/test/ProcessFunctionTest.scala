package com.yaowb.flink.test

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 当温度有改变时进行报警
 */
object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(100000)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(300), Time.seconds(10)))


    val stream = env.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading]() {
        override def extractTimestamp(t: SensorReading): Long = t.timeStamp * 1000
      })
    val processedStream = dataStream.keyBy(_.id)
      .process(new TempIncreAlert)

    val processedStream2=dataStream.keyBy(_.id)
//      .process(new TempChangeAlert(10.0))
      .process(new TempChangeAlert2(10.0))

    val processedStream3=dataStream.keyBy(_.id)
        .flatMapWithState[(String,Double,Double),Double]{
          //如果没有状态的话，也就是没有数据过来，那么就将当前数据温度值存入状态
          case(input:SensorReading,None)=>(List.empty,Some(input.temperature))
          case(input: SensorReading,lastTemp:Some[Double])=>
            val diff=(input.temperature-lastTemp.get).abs
            if (diff>10.0){
              (List((input.id,lastTemp.get,input.temperature)),Some(input.temperature))
            }else {
              (List.empty, Some(input.temperature))
            }
        }

    dataStream.print("input data")

    processedStream3.print("processed data")

    env.execute("process function test")
  }
}

/**
 * 温度增长报警事件
 */
class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {

  //定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTime", classOf[Double]))

  //定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTime", classOf[Long]))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {

    //去出上一个温度值
    val preTemp = lastTemp.value()
    //更新温度值
    lastTemp.update(i.temperature)

    val curTimerTs = currentTimer.value()

    if (i.temperature > preTemp || preTemp == 0.0) {
      //如果温度下降，或当前是第一条数据，删除定时器并清空状态
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    } else if (i.temperature > preTemp && curTimerTs==0) {
      //温度上升且没有设过定时器，则注册定时器
      val timerTs = context.timerService().currentProcessingTime() + 5000L;
      context.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //输出报警信息
    out.collect(ctx.getCurrentKey+"温度连续上升")
    currentTimer.clear()
  }
}

/**
 * 温度变化事件，当温度变化值大于指定值的时候，发出警报
 */
class TempChangeAlert(threshold:Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
  private var lastTempState:ValueState[Double]=_

  override def open(parameters: Configuration): Unit = {
    //初始化的时候声明state变量
    lastTempState= getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值

    val lastTemp = lastTempState.value()

    //当温度变化值大于指定值的时候，发出警报
    val diff=(value.temperature-lastTemp).abs
    if (diff>threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lastTempState.update(value.temperature)
  }
}

/**
 * 温度变化事件，当温度变化值大于指定值的时候，发出警报
 * @param threshold
 */
class TempChangeAlert2(threshold:Double) extends KeyedProcessFunction[String,SensorReading,(String,Double,Double)]{

  //定义一个变量，保存上次的温度值
  lazy val lastTempState:ValueState[Double]= getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp= lastTempState.value()

    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff=(i.temperature-lastTemp).abs
    if (diff!=threshold) {
      collector.collect(i.id, lastTemp, i.temperature)
    }

    lastTempState.update(i.temperature)
  }
}
