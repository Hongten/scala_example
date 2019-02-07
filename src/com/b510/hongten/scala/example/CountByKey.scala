/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * countByKey : Action算子， 对RDD中的Key进行计数
 * @author Hongten
 * @created 7 Feb, 2019
 */
object CountByKey {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("countByKey")

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

    val result = data.countByKey

    /**
     * output:
     * key : Tom, value : 3
     * key : Cat, value : 2
     * key : Hongten, value : 2
     */
    for ((k, v) <- result) {
      println("key : " + k + ", value : " + v)
    }

    sc.stop
  }
}