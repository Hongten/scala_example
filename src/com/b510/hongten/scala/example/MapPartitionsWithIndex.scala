/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 该方法可以获取到RDD中的partition索引
 *
 * @author Hongten
 * @created 6 Feb, 2019
 */
object MapPartitionsWithIndex {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("MapParittionsWithIndex")

    val sc = new SparkContext(conf)

    val list = Array("a", "b", "c", "d", "e", "f", "g", "h")

    val rdd = sc.parallelize(list, 3)

    println("partition size : " + rdd.partitions.size)

    val resutl = rdd.mapPartitionsWithIndex(myfunc, true).collect;

    println("----------------------")

    /**
     * output:
     * partition index : 0, value : b
     * partition index : 0, value : a
     * partition index : 1, value : e
     * partition index : 1, value : d
     * partition index : 1, value : c
     * partition index : 2, value : h
     * partition index : 2, value : g
     * partition index : 2, value : f
     */

    if (resutl != null && resutl.size > 0) {
      for (v <- resutl) {
        println(v)
      }
    }

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