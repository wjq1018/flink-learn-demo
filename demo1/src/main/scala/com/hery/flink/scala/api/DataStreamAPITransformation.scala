package com.hery.flink.scala.api

import com.hery.flink.java.utils.WordCountData
import com.hery.flink.scala.partitioner.MyPartitioner
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction

/**
 * @Date 2020/8/19
 * @Created by hery
 * @Description flink 的Datastream 转换操作
 */
object DataStreamAPITransformation {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    partitonOPS(env)
    //    env.execute("DataStreamAPITransformation")
  }

  def baseOPS(env: StreamExecutionEnvironment): Unit = {
    env.setParallelism(1)
    val data: DataStream[(Int, Int)] = env.fromElements(Tuple2(1, 1), Tuple2(1, 2), Tuple2(2, 1), Tuple2(2, 5))
    //    val strdata = env.fromElements("hello,you,hello,me")
    //    // map操作
    //    val result = data.map(x => (x._1, x._2 * 2))
    //    result.print()
    //    // filter 操作
    //    val filtereddata = data.filter(x => x._1 > 1)
    //    // flatMap操作
    //    strdata.flatMap(x => x.split(",")).print()
    //keyBy
    //    data.keyBy(0).print()
    // aggregations 操作，常用分装函数如sum,min,max,minBy,maxBy等
    data.keyBy(0).min(1).print()
  }

  def joinOPS(env: StreamExecutionEnvironment): Unit = {
    //    val data: DataStream[(Int, Int)] = env.fromElements(Tuple2(1, 1), Tuple2(1, 2), Tuple2(2, 1), Tuple2(2, 2))
    //    val num: DataStream[(Int, Int)] = env.fromElements(Tuple2(1, 1), Tuple2(2, 1), Tuple2(3, 1), Tuple2(4, 1))
    //    //union
    //    data.union(num).print()


    //    val first: DataStream[(Int, Long)] = env.fromElements((1,1.0),(2,2.0),(3,3.0))
    //    val second: DataStream[(Int, String)] = env.fromElements((1,"tom"),(2,"lucy"),(3,"lily"))
    // connect streams with keyBy
    //    val keyedConnect: ConnectedStreams[(Int, Long), (Int, String)] = first
    //      .connect(second)
    //      .keyBy(0, 0) // key both input streams on first attribute

    // connect streams with broadcast
    //    val value = first.connect(second.broadcast())

    // split 操作
    val inputStream: DataStream[(Int, String)] = env.fromElements((1, "tom"), (2, "lucy"), (3, "lily"))
    val splitted: SplitStream[(Int, String)] = inputStream
      .split(s => if (s._1 % 2 == 0) Seq("even") else Seq("odd"))

    val even: DataStream[(Int, String)] = splitted.select("even")
    val odd: DataStream[(Int, String)] = splitted.select("odd")
    val all: DataStream[(Int, String)] = splitted.select("odd", "even")
  }

  /**
   * 分区操作
   *
   * @param env
   * 参考：https://zhuanlan.zhihu.com/p/87752288
   * https://zhuanlan.zhihu.com/p/165907184
   */
  def partitonOPS(env: StreamExecutionEnvironment): Unit = {
    // 获取数据
    val data = env.fromElements(
      (1, "beijing"),
      (2, "beijing"),
      (3, "shanghai"),
      (3, "shanghai"),
      (4, "zhangjiakou"),
      (3, "shanghai"),
      (7, "liaoning"),
      (3, "shanghai"),
      (5, "nanjing"),
      (5, "nanjing")
    )
    data.map(x => {
      println("00线程ID:" + Thread.currentThread().getId + "****" + x._1)
      (x)
    }).shuffle.print()
    //rebalance 方式
//    data.rebalance.print()
//
//    //rescale 方式
//    data.rescale.print()
//
//    //broadcast 方式
//    data.broadcast.print()
//
//    // 自定义方式
//    val stream: DataStream[(Int)] = env.fromElements(1, 2, 3, 4, 5)
//    stream.partitionCustom(MyPartitioner, 0)




  }


}
