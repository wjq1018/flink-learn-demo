package com.hery.flink.scala.api

import com.hery.flink.java.output.MyOutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * @Date 2020/7/22 10:50
 * @Created by hery
 * @Description
 * flink 的DataSet sink 操作
 */
object DataSetAPISink {
  def main(args: Array[String]): Unit = {
//        pirntSink()
//        fileSink()
        outputSink()

  }

  def pirntSink(): Unit = {
    // 创建环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    从Seq中创建DataSet数据集
    val seq: DataSet[String] = env.fromCollection(Seq("lillcol", "study", "flink"))
    // 打印输出
    seq.print()
  }

  /**
   * 使用自定义的outputformat 来进行内容的输出
   */
  def outputSink(): Unit = {
    // 创建环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    从Seq中创建DataSet数据集

    val initialData = env.fromElements((3.141592, "foobar", 77L)).setParallelism(1)

    initialData.map((v1) => v1).output(new MyOutputFormat[(Double, String, Long)])

    env.execute()
  }

  /**
   * 基于文件类的输出
   */
  def fileSink(): Unit = {
    // 创建环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    从Seq中创建DataSet数据集
    val seq: DataSet[String] = env.fromCollection(Seq("lillcol", "study", "flink"))
    seq.print()
    val result: DataSet[String] = seq.map(_.toUpperCase)
    // 将数据输出 可以选择写入模式：OVERWRITE  NO_OVERWRITE(默认)
    result.writeAsText("D:\\data\\output\\flinkout.txt", WriteMode.OVERWRITE)

    val csvdata = seq.map((_, 1))
    csvdata.writeAsCsv("D:\\data\\output\\flinkout.csv")

    // 触发计算
    env.execute()
  }
}
