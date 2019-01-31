/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * Action算子：获取数据集
 *
 * 会将Worker端的结果回收到Driver端
 *
 * @author Hongten
 * @created 31 Jan, 2019
 */
object Collect {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("collect")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("./resources/test_data").collect

    println(lines)

    sc.stop
  }
}