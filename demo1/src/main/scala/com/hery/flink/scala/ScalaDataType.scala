package com.hery.flink.scala

/**
 * @Date 2020/7/2 18:01
 * @Created by hery
 * @Description 数据类型
 *             Option 有两个子类别，一个是 Some，一个是 None，当他回传 Some 的时候，代表这个函式成功地给了你一个 String，
 *             而你可以透过 get() 这个函式拿到那个 String，如果他返回的是 None，则代表没有字符串可以给你
 */
object ScalaDataType {
  def main(args: Array[String]): Unit = {
    // 虽然 Scala 可以不定义变量的类型，不过为了清楚些，我还是
    // 把他显示的定义上了

    val myMap: Map[String, String] = Map("key1" -> "value")
    //Option[String] 来告诉你：「我会想办法回传一个 String，但也可能没有 String 给你」
    val value1: Option[String] = myMap.get("key1")
    val value2: Option[String] = myMap.get("key2")

    println(value1) // Some("value1")
    println(value2) // None
  }

  def show(x: Option[String]) = x match {
    case Some(s) => s
    case None => "?"
  }
}
