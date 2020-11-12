package com.hery.flink.scala

import com.hery.flink.java.utils.WordCountData
import org.apache.flink.streaming.api.scala._


/**
 * @Date 2020/7/1 16:50
 * @Created by hery
 * @Description flink scala 流处理
 */
object StreamingJob {
  def main(args: Array[String]): Unit = {

    // 设置环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 数据读取
    val text = env.fromElements(WordCountData.WORDS: _*)

    // 数据处理
    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .sum(1)

    // 数据输出
    counts.print()
    // 执行程序
    env.execute("StreamingJob WordCount")
  }
}
