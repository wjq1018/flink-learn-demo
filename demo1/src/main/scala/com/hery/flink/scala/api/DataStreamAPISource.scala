package com.hery.flink.scala.api


import java.util.Properties

import com.hery.flink.java.source.SourceFromMySQL
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @Date 2020/8/19
 * @Created by hery
 * @Description flink DataStream API Source
 */
object DataStreamAPISource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    dataFromCollect(env)
    // 执行任务
    env.execute("DataStreamAPISource")
  }

  /**
   * flink 内置数据源
   * source为file类型
   *
   * @param env
   */
  def dataFromFile(env: StreamExecutionEnvironment): Unit = {
    val value = env.readTextFile("D:\\hery\\flink_learning\\code\\flink_study\\src\\main\\resources\\log4j.properties")
    //输出结果
    value.print()
    // readFile 参考DataStreamAPISourceJava.java类
  }

  /**
   * 读取socket数据
   *
   * @param env
   */
  def dataFromSocket(env: StreamExecutionEnvironment): Unit = {
    // 监听某个机器的的某个端口，从socket中读取数据
    //nc -l -p 9000 -v
    val data = env.socketTextStream("127.0.0.1", 9000)
    data.print()
  }

  /**
   * 从集合中读取数据
   *
   * @param env
   */
  def dataFromCollect(env: StreamExecutionEnvironment): Unit = {

    // 从集合中读取
    val data: DataStream[(Int, Int)] = env.fromElements(Tuple2(1, 2), Tuple2(1, 2), Tuple2(1, 2), Tuple2(1, 2))
    data.print()
    //
    import org.apache.flink.streaming.api.datastream.DataStreamSource

    val strArr = Array[String]("spark", "flink")
    val arr = env.fromCollection(strArr)
    arr.print()

  }

  /**
   * 读取来自kafka的数据
   *
   * @param env
   */
  def dataFromKafka(env: StreamExecutionEnvironment): Unit = {

    // kafka topic
    val topic = "topic-calculate-brinson"
    val prop = new Properties
    prop.setProperty("bootstrap.servers", "192.168.100.105:9092,192.168.100.106:9092,192.168.100.107:9092")
    prop.setProperty("group.id", "flink_test_group")
    val kafkaConsumer011 = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema, prop)
    kafkaConsumer011.setStartFromEarliest
    kafkaConsumer011.setCommitOffsetsOnCheckpoints(false) //取消提交Offset与设置检查点之间的关联。

    val text = env.addSource(kafkaConsumer011)
    text.print
  }

  /**
   * 读取来自msyql 数据
   *
   * @param env
   */
  def dataFromJDBC(env: StreamExecutionEnvironment): Unit = {
    // 创建jdbc input format
    val jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://ip:3306/tablename?characterEncoding=utf8")
      .setUsername("root")
      .setPassword("root")
      .setQuery("select * from tmp")
      .finish()
    // 读取数据
    val data = env.createInput(jdbcInputFormat)
    data.print()
  }

  /**
   * 使用自定义数据源
   *
   * @param env
   */
  def dataFromRichSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new SourceFromMySQL())
    data.print()
  }


}
