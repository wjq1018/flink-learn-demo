package com.hery.flink.scala.api

import org.apache.flink.api.common.functions.{Partitioner, RichMapFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.math.Ordering.String
import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.util.Random

/**
 * @Date 2020/7/8 17:32
 * @Created by hery
 * @Description flink dataset 转换操作
 */
object DataSetAPITransformation {
  def main(args: Array[String]): Unit = {
    //    val env = ExecutionEnvironment.getExecutionEnvironment
    //        baseOPS(env)
    //    aggOPS()
    //    joinOPS()
    //    collectionOPS()
    sortOPS()
  }

  /**
   * DataSet的基本操作
   */
  def baseOPS(env: ExecutionEnvironment): Unit = {
    // 设置环境变量
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 读取数据
    val num = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
    val text = env.fromCollection(Array("Hello,you", "hello,me"))

    // map 操作
    val numMap = num.map(x => x * 2)
    //    numMap.print()


    // flatMap 操作
    val counts = text.flatMap(x => {
      if (x.length > 2) {
        x.toLowerCase().split(",")
      } else {
        Seq.empty
      }
    })
    //    counts.print()

    // mapPartition 操作
    val numPartitons = num.mapPartition(x => x.map(_ * 2))
    numPartitons.print()
    // filter 操作
    val filterVal = num.filter(x => x % 2 == 0) // 返回为true的元素
    //    filterVal.print()

  }


  /**
   * 聚合操作
   */
  def aggOPS(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 读取数据
    val num = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
    // reduce 操作
    val reduceVal = num.reduce(_ + _)
    // 写法二
    val reduceVal_1 = num.reduce((x, y) => {
      x + y
    })
    //    reduceVal.print()

    //    // reduceGroup操作
    val reduceGroupValue = num.reduceGroup(x => x.sum)
    //    reduceGroupValue.print()

    //aggregate 操作
    val input: DataSet[(Int, String, Double)] = env.fromElements(
      (1, "tom", 10d), (1, "lucy", 20d), (2, "zhangsan", 30d), (2, "lisi", 50d)
    )

    val output: DataSet[(Int, String, Double)] = input.aggregate(Aggregations.SUM, 0)
    //output.sum(0)  aggregate(Aggregations.SUM, field)
    output.print() //(6,lisi,50.0)

    // distinct 操作
    //    num.distinct()
  }

  /**
   * 关联操作
   */
  def joinOPS(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val user: DataSet[(Int, String, Int)] = env.fromElements(
      (1, "tom", 1), (2, "lucy", 2), (3, "zhangsan", 3), (4, "lisi", 1)
    )

    val city: DataSet[(Int, String)] = env.fromElements(
      (1, "beijing"), (2, "beijing"), (3, "shanghai"), (3, "shanghai")
    )
    // join 操作
    user.join(city).where(0).equalTo(0) { (l, r) => (l._1, l._2, r._2) }.print()
    // 第二个数据集是小数据集
    user.joinWithTiny(city).where(0).equalTo(0) { (l, r) => (l._1, l._2, r._2) }.print()
    // 第二个数据集市大数据集
    user.joinWithHuge(city).where(0).equalTo(0) { (l, r) => (l._1, l._2, r._2) }.print()

    // 广播第一个数据集来执行连接，对广播的数据集使用散列表(散列函数)
    user.join(city, JoinHint.BROADCAST_HASH_FIRST).where(0).equalTo(0)
    // JoinHint 的方式有下面6种:
    //    OPTIMIZER_CHOOSES
    //    BROADCAST_HASH_FIRST
    //    BROADCAST_HASH_SECOND
    //    REPARTITION_HASH_FIRST
    //    REPARTITION_HASH_SECOND
    //    REPARTITION_SORT_MERGE

    // outer join
    // 默认没有匹配数据填充空值
    user.leftOuterJoin(city, JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(0) { (l, r) => (l._1, l._2, r._2) }.print()
    // 自定义输出,设定没有匹配的时候的值
    user.rightOuterJoin(city, JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(0) { (l, r) => if (r == null) (l._1, l._2, "未知") else (l._1, l._2, r._2) }.print()
    // 自定义输出，设定没有匹配的时候的值
    user.fullOuterJoin(city, JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(0) { (l, r) => if (r == null) (l._1, l._2, "未知") else (l._1, l._2, r._2) }.print()

    //coGroup 操作 将两个数据集根据key组合在一起，相同的可以会放在一个Group中
    user.coGroup(city).where(0).equalTo(0).map(x => (x._1.mkString("#"), x._2.mkString("**"))).print()

    // cross 操作 将两个数据集合并成一个数据集，合并的结果是两个数据集的笛卡尔集
    user.cross(city).print()
  }

  /**
   * 集合操作
   */
  def collectionOPS(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val num1 = env.fromElements(1, 2, 3)
    val num2 = env.fromElements(4, 5, 6)
    // union 操作 两个数据集类型必须相同
    //    num1.union(num2).print()


    //Rebalance 操作
    val ds = env.generateSequence(1, 3000)
    val skewed = ds.filter(_ > 600)
    val rebalanced = skewed.rebalance()
    val countsInPartition = rebalanced.map(new RichMapFunction[Long, (Int, Long)] {
      def map(in: Long) = {
        (getRuntimeContext.getIndexOfThisSubtask, 1)
      }
    })
      .groupBy(0)
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
      .map { in => (in._1, in._2 / 10) }
    countsInPartition.print()

    val user: DataSet[(Int, String, Int)] = env.fromElements(
      (1, "tom", 1), (2, "lucy", 2), (3, "zhangsan", 3), (4, "lisi", 1), (4, "lisi", 1)
    )
    user.partitionByHash(1).mapPartition(s => {
      s.toIterator
    }).print()


    // partitionByRange 操作

    val city: DataSet[(Int, String)] = env.fromElements(
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
    // partitionByRange作用是把key相同的元素放入同一个分区（由同一个线程处理）
    //注意：是相同的key，不是hash值相同的key
    import org.apache.flink.api.common.operators.Order
    city.partitionByRange(0).sortPartition(0, Order.DESCENDING).map(x => {
      println("线程ID:" + Thread.currentThread().getId + "****" + x._1)
      (x)
    }).print()


    //sortPartition默认由一个线程处理，是全局有效，保证整个数据集有序 默认asc
    //sortPartition的默认并行度是1，是全局有序，
    //或者
    //    sortPartition(0, Order.DESCENDING).setParallelism(1)并行度是1，也是全局有序
    city.sortPartition(0, Order.DESCENDING).map(x => {
      println("线程ID:" + Thread.currentThread().getId + "****" + x._1)
      (x)
    }).print()

    val data = env.fromElements((0, 0)).rebalance()
    data.partitionCustom(new TestPartitionerLong(), 0).print()

  }

  /**
   * 排序操作
   */
  def sortOPS(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input: DataSet[(Int, String, Double)] = env.fromElements(
      (1, "tom", 10d), (1, "lucy", 20d), (2, "zhangsan", 30d), (2, "lisi", 50d)
    )
    input.print()
    // first-N操作
    // 作用在所有数据集上
    input.first(2).print()
    // 作用在分组数据集上
    input.groupBy(0).first(1).print()
    // 在分组排序的基础上进行top N
    input.groupBy(0).sortGroup(1, Order.ASCENDING).first(3).print()

    // minBy MaxBy
    val data: DataSet[(Int, String, Double)] = env.fromElements(
      (1, "tom", 10d), (1, "lucy", 20d), (2, "zhangsan", 30d), (2, "lisi", 50d)
    )
    //取第一第二个字段的最小值
    //    data.minBy(0,2).print() //(1,tom,10.0)
    //根据第一个字段分组，去第二个字段最小值
    data.groupBy(0).min(2).print()
  }
}

/**
 * 自定义分区器
 */
class TestPartitionerLong extends Partitioner[Long] {

  override def partition(key: Long, numPartitions: Int): Int = 0
}
