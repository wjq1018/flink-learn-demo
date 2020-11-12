package com.hery.flink.scala.api

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.StringValue
import org.apache.flink.api.scala._

/**
 * @Date 2020/6/29 14:42
 * @Created by hery
 * @Description flink  dataset  读取数据
 */
object DataSetAPISource {

  def main(args: Array[String]): Unit = {
    readdataFromCollection()
  }

  /**
   * 从文件读取
   */
  def readDataFromFile(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 读取本地文件
    val data = env.readTextFile("D:\\data\\test.data")
    // 读取hdfs文件
    val hfdsdata = env.readTextFile("hdfs:///warehouse/test/test.data")

    //读取本地文件 ，返回StringValue,并且指定字符格式为UTF-8
    val strdat: DataSet[StringValue] = env.readTextFileWithValue("D:\\data\\test.data", "UTF-8")

    // 引入隐式转换
    import org.apache.flink.api.scala._
    //读取包含三个字段的CSV文件
    val csvInput: DataSet[(Int, String, String)] = env.readCsvFile[(Int, String, String)]("hdfs:///the/CSV/file")
    //includedFields 指选择指定位置的数据
    val myCaseClass: DataSet[(Int, Int)] = env.readCsvFile[(Int, Int)]("D:\\warehouse\\data\\test\\testcsv", includedFields = Array(0, 3))
    //CSV输入还可以与Case类一起使用要注意case class 定义的位置
    case class MyCaseClass(str: Int, dbl: Int)
    val csvInput1 = env.readCsvFile[MyCaseClass](
      "hdfs:///the/CSV/file",
      includedFields = Array(0, 3))
  }

  /**
   * 从集合读取数据
   */
  def readdataFromCollection(): Unit = {
    // 创建环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //    从Seq中创建DataSet数据集
    val seq: DataSet[String] = env.fromCollection(Seq("lillcol", "study", "flink"))
    println("seq: ")
    seq.print()
    println()
    // 从Iterator中创建数据集
    val iterator: DataSet[String] = env.fromCollection(Iterator("lillcol", "study", "flink"))
    println("iterator: ")
    iterator.print()
    println()

    // 从元素从创建数据集
    val fromElements: DataSet[Any] = env.fromElements(1, 2, 3, 4)
    println("fromElements: ")
    fromElements.print()
    println()
    //   并行地生成给定区间内的数字序列。
    val generateSequence: DataSet[Long] = env.generateSequence(1, 10)
    println("generateSequence: ")
    generateSequence.print()
    println()
  }


  /**
   * 使用JDBC输入格式从关系数据库读取数据
   */
  def readDataFromCreateInput(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputMysql = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      // 数据库连接驱动名称
      .setDrivername("com.mysql.jdbc.driver")
      // 数据库连接驱动名称
      .setDBUrl("jdbc:mysql://localhost:3306/flink")
      // 数据库连接用户名
      .setUsername("root")
      // 数据库连接密码
      .setPassword("root")
      // 数据库连接查询SQL
      .setQuery("select name,age,class from test")
      // 字段类型,顺序个个数必须与SQL保持一致
      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
      .finish()
    )

    inputMysql.print()
  }

  /**
   * 递归读取文件
   */
  def readDataFromFileRecursion(): Unit = {
    // 获取环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 创建一个配置对象
    val parameters = new Configuration
    // 设置递归枚举参数
    parameters.setBoolean("recursive.file.enumeration", true)
    // 将配置传递给数据源
    val input: DataSet[String] = env.readTextFile("D:\\flink\\data\\test")
      .withParameters(parameters) //路径为一个目录
    //输出结果
    import org.apache.flink.api.scala._
    input.flatMap(_.split(",")).map((_, 1)).groupBy(0).sum(1).print()
  }

}
