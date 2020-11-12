package com.hery.flink.scala


import com.hery.flink.java.utils.WordCountData
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @Date 2020/7/1 16:50
 * @Created by hery
 * @Description flink scala  版本 批处理
 */
object BatchJob {
  def main(args: Array[String]): Unit = {

    // 设置环境变量
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 读取数据
    val text = env.fromCollection(WordCountData.WORDS)
    // 数据处理
    val counts = text.flatMap(x => x.toLowerCase().split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    // 数据输出
    counts.print()
  }
}
