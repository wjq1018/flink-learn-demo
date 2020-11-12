package com.hery.flink.scala.partitioner

import org.apache.flink.api.common.functions.Partitioner

/**
 * @Date 2020/8/20 17:47
 * @Created by hery
 * @Description
 */
object MyPartitioner extends Partitioner[Int] {
  val r = scala.util.Random

  override def partition(key: Int, numPartitions: Int): Int = {
    if (key < 0) 0 else r.nextInt(numPartitions)
  }
}