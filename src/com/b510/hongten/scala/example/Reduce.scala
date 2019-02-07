/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * reduce: Action算子，根据聚合逻辑聚合数据集中的每个元素
 * 
 * @author Hongten
 * @created 7 Feb, 2019
 */
object Reduce {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("reduce")

    val sc = new SparkContext(conf)

    val arrayData = List(2, 5, 3, 6)

    val result = sc.parallelize(arrayData, 3).reduce(_ * _)

    /**
     * output:
     * result : 180
     */
    println("result : " + result)
    sc.stop

  }

}