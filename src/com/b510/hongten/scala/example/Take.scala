/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Action算子：获取RDD结果前n行
 *
 * @author Hongten
 * @created 31 Jan, 2019
 */
object Take {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("take")

    val sc = new SparkContext(conf)

    //rdd.first = rdd.take(1)
    val takeResult = sc.textFile("./resources/test_data").take(2)

    takeResult.foreach(println)

    sc.stop
  }

}