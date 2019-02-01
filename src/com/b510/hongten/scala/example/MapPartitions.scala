/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * https://stackoverflow.com/questions/21185092/apache-spark-map-vs-mappartitions
 *
 * mapPartition和map的区别：
 * 1.map是一条一条处理数据
 * 2.mapPartition是安装分区partition来处理数据
 *
 * 场景： 如果数据量很大，推荐使用mapPartition
 *
 * @author Hongten
 * @created 1 Feb, 2019
 */
object MapPartitions {

  def main(args: Array[String]) {

    val conf = new SparkConf
    conf.setMaster("local").setAppName("left join")

    val sc = new SparkContext(conf)

    val x = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

    println("----------- 与map进行对比 ------------")
    println("----------- map是一条一条处理数据 ------------")
    println("----------- mapPartition是安装分区partition来处理数据 ------------")
    println("----------- 如果数据量很大，推荐使用mapPartition ------------")
    x.map(num => {
      println("map - 打开数据库连接.....")
      println("map - 处理data ..... " + num)
      println("map - 关闭数据库连接.....")
    }).collect

    println("----------- 分割线 ------------")
    val result = x.mapPartitions(myfunc).collect
    for (x <- result) {
      println(x)
    }

    println("----------- 分割线 ------------")
    println("----------- foreachPartition不会返回数据，是Action算子，安装分区来遍历数据 ------------")
    x.foreachPartition(myfunc)

    sc.stop

  }

  def myfunc(iter: Iterator[Int]): Iterator[Int] = {
    var res = List[Int]()
    println("打开数据库连接.....")
    while (iter.hasNext) {
      val cur = iter.next;
      println("批处理data ..... " + cur)
    }
    println("关闭数据库连接.....")
    res.iterator
  }
}