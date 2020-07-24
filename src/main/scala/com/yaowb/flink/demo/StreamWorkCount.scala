package com.yaowb.flink.demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 流处理文件
 */
object StreamWorkCount {

  def main(args: Array[String]): Unit = {

    val param = ParameterTool.fromArgs(args)
    val host: String = param.get("host")
    val port: Int = param.getInt("port")

    //流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


//    env.disableOperatorChaining()  //禁用数据链 完全打散所有任务算子

    val textDataStream = env.socketTextStream("localhost", 7777)

    val wordCountDataStream = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty) //过滤非空字段
//      .startNewChain()//手动添加数据链
//      .disableChaining()//禁用当钱filter相关的数据链
      .map((_, 1)) //整理进map
      .keyBy(0) //根据对应的单词group
      .sum(1) //累加

    //打印结果
    wordCountDataStream.print().setParallelism(1)

    //执行任务
    env.execute("stream word count job!")
  }

}
