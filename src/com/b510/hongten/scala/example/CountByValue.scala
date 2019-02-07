/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * countByValue: Action算子，对数据集中个元素进行计数
 *
 * @author Hongten
 * @created 7 Feb, 2019
 */
object CountByValue {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("countByValue")

    val sc = new SparkContext(conf)

    val dataList = List(
      new Tuple2("Hongten", 20),
      new Tuple2("Hongten", 12),
      new Tuple2("Tom", 12),
      new Tuple2("Tom", 12),
      new Tuple2("Tom", 12),
      new Tuple2("Cat", 12),
      new Tuple2("Cat", 30))

    val data = sc.parallelize(dataList, 3)

    val result = data.countByValue

    /**
     * output:
     * key : (Cat,30), value : 1
     * key : (Cat,12), value : 1
     * key : (Hongten,20), value : 1
     * key : (Tom,12), value : 3
     * key : (Hongten,12), value : 1
     */
    for ((k, v) <- result) {
      println("key : " + k + ", value : " + v)
    }

    sc.stop
  }
}