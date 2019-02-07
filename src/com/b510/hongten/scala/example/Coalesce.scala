/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * coalesce : 合并
 * 
 * 2：分区数
 * false: 是否要shuffle,默认为false，即不产生shuffle
 * 如果为true的时候，即repartition()
 * 
 * coalesce(2, false)
 * 
 * @author Hongten
 * @created 7 Feb, 2019
 */
object Coalesce {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("coalesce")

    val sc = new SparkContext(conf)

    val list = Array("a", "b", "c", "d", "e", "f", "g", "h")

    val rdd = sc.parallelize(list, 3)

    println("partition size : " + rdd.partitions.size)

    val resutl = rdd.mapPartitionsWithIndex(myfunc, true)

    //对RDD重新分区
    //2: 分区数
    //false: 是否要shuffle,默认为false，即不产生shuffle
    //如果为true的时候，即repartition()
    val coalesceResult = resutl.coalesce(2, false)

    coalesceResult.mapPartitionsWithIndex(myfunc, true).collect

    sc.stop

  }

  def myfunc(index: Int, iter: Iterator[String]): Iterator[String] = {
    var res = List[String]()
    while (iter.hasNext) {
      val cur = iter.next;
      val valueString = "partition index : " + index + ", value : " + cur;
      println(valueString);
      res = valueString :: res
    }
    res.iterator
  }
}