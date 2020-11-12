package com.hery.flink.scala

import com.hery.flink.scala.case_class.Person
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Date 2020/7/2 16:49
 * @Created by hery
 * @Description
 */
object FlinkDataType {

  def main(args: Array[String]): Unit = {
    // 获取环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val persons = Array[Person](Person("zhangsan", 23), Person("lisi", 22))
    // 读取数据
    val personDS: DataSet[Person] = env.fromCollection(persons)
    // 输出数据
    personDS.print()


  }
}