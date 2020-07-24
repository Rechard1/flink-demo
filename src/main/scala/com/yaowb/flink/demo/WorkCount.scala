package com.yaowb.flink.demo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


//批处理
object WorkCount {
  def main(args: Array[String]): Unit = {

    //创建执行环境
    val evn = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath = "D:\\java-work\\FlinkDemo\\src\\main\\resources\\hello.txt"

    val inputData = evn.readTextFile(inputPath)

    //flatMap先把单次打散，接着放进map中（单词本身，数量），最后为单词分组做统计
    val wordCount = inputData.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    wordCount.print()
  }
}
